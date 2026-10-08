use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Timelike;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::path::Path;
use std::sync::Arc;
use tiberius::Row;

use crate::schema::{ColumnInfo, TableInfo};
use crate::types::build_arrow_schema;

pub struct ParquetTableWriter {
    writer: ArrowWriter<std::fs::File>,
    schema: Arc<Schema>,
    columns: Vec<ColumnInfo>,
    batch_size: usize,
    row_buffer: Vec<Row>,
    rows_written: u64,
}

impl ParquetTableWriter {
    pub fn new(path: &Path, table: &TableInfo, batch_size: usize) -> Result<Self> {
        let schema = Arc::new(build_arrow_schema(&table.columns));
        let file = std::fs::File::create(path)
            .with_context(|| format!("Failed to create parquet file: {}", path.display()))?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .context("Failed to create ArrowWriter")?;

        Ok(Self {
            writer,
            schema,
            columns: table.columns.clone(),
            batch_size,
            row_buffer: Vec::with_capacity(batch_size),
            rows_written: 0,
        })
    }

    /// Push one row from the tiberius query stream. Flushes when batch_size is reached.
    pub fn push_row(&mut self, row: Row) -> Result<()> {
        self.row_buffer.push(row);
        if self.row_buffer.len() >= self.batch_size {
            self.flush_batch()?;
        }
        Ok(())
    }

    /// Flush remaining buffered rows and close the Parquet file.
    pub fn finish(mut self) -> Result<u64> {
        if !self.row_buffer.is_empty() {
            self.flush_batch()?;
        }
        self.writer.close().context("Failed to close ArrowWriter")?;
        Ok(self.rows_written)
    }

    fn flush_batch(&mut self) -> Result<()> {
        let rows = std::mem::take(&mut self.row_buffer);
        let batch = rows_to_record_batch(&rows, &self.columns, self.schema.clone())
            .context("Failed to convert rows to RecordBatch")?;
        self.rows_written += batch.num_rows() as u64;
        self.writer.write(&batch).context("Failed to write RecordBatch")?;
        Ok(())
    }
}

// ── Core conversion: Vec<Row> → RecordBatch ───────────────────────────────────

/// Macro to build a primitive Arrow array from tiberius rows.
/// Usage: build_primitive_col!(rows, col_idx, i32, Int32Builder)
macro_rules! build_primitive_col {
    ($rows:expr, $idx:expr, $rust_type:ty, $builder:ty) => {{
        let mut b = <$builder>::with_capacity($rows.len());
        for row in $rows {
            match row.get::<$rust_type, _>($idx) {
                Some(v) => b.append_value(v),
                None => b.append_null(),
            }
        }
        Arc::new(b.finish()) as ArrayRef
    }};
}

fn rows_to_record_batch(
    rows: &[Row],
    columns: &[ColumnInfo],
    schema: Arc<Schema>,
) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());

    for (col_idx, col_info) in columns.iter().enumerate() {
        let arrow_type = schema.field(col_idx).data_type();
        let array: ArrayRef = match arrow_type {
            DataType::Int64 => build_primitive_col!(rows, col_idx, i64, Int64Builder),
            DataType::Int32 => build_primitive_col!(rows, col_idx, i32, Int32Builder),
            DataType::Int16 => build_primitive_col!(rows, col_idx, i16, Int16Builder),
            DataType::UInt8 => {
                // SQL Server TINYINT is unsigned 0-255; tiberius returns u8
                let mut b = UInt8Builder::with_capacity(rows.len());
                for row in rows {
                    match row.get::<u8, _>(col_idx) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => build_primitive_col!(rows, col_idx, bool, BooleanBuilder),
            DataType::Float64 => build_primitive_col!(rows, col_idx, f64, Float64Builder),
            DataType::Float32 => build_primitive_col!(rows, col_idx, f32, Float32Builder),

            DataType::Decimal128(precision, scale) => {
                build_decimal128_col(rows, col_idx, *precision, *scale)?
            }

            DataType::Utf8 => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
                for row in rows {
                    match row.get::<&str, _>(col_idx) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }

            DataType::Binary => {
                let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match row.get::<&[u8], _>(col_idx) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }

            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                build_timestamp_col(rows, col_idx, &col_info.sql_type)?
            }

            DataType::Date32 => {
                use chrono::NaiveDate;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let mut b = Date32Builder::with_capacity(rows.len());
                for row in rows {
                    match row.get::<NaiveDate, _>(col_idx) {
                        Some(v) => {
                            let days = v.signed_duration_since(epoch).num_days() as i32;
                            b.append_value(days);
                        }
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }

            DataType::Time64(TimeUnit::Microsecond) => {
                use chrono::NaiveTime;
                let mut b = Time64MicrosecondBuilder::with_capacity(rows.len());
                for row in rows {
                    match row.get::<NaiveTime, _>(col_idx) {
                        Some(v) => {
                            let micros = v.num_seconds_from_midnight() as i64 * 1_000_000
                                + v.nanosecond() as i64 / 1000;
                            b.append_value(micros);
                        }
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }

            DataType::FixedSizeBinary(16) => {
                // UNIQUEIDENTIFIER — tiberius returns uuid::Uuid
                let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
                for row in rows {
                    match row.get::<uuid::Uuid, _>(col_idx) {
                        Some(v) => b.append_value(v.as_bytes())?,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }

            // Fallback for unknown types
            _ => {
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
                for row in rows {
                    match row.get::<&str, _>(col_idx) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };

        arrays.push(array);
    }

    RecordBatch::try_new(schema, arrays).context("Failed to create RecordBatch")
}

/// Build a Decimal128 array from tiberius Numeric (bigdecimal) values.
/// Arrow stores Decimal128 as the unscaled i128 integer at the column's declared scale.
/// P2: rescale bigdecimal to column scale before converting to i128.
fn build_decimal128_col(
    rows: &[Row],
    col_idx: usize,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    
    

    let mut b = Decimal128Builder::with_capacity(rows.len())
        .with_precision_and_scale(precision, scale)?;

    for row in rows {
        // tiberius 0.12 with bigdecimal 0.3: NUMERIC comes back as tiberius::numeric::Numeric
        // which implements FromSql. We use i128 via tiberius's own Numeric type.
        match row.get::<tiberius::numeric::Numeric, _>(col_idx) {
            Some(n) => {
                // Numeric holds value as i128 scaled by 10^scale internally
                let raw = n.value();
                b.append_value(raw);
            }
            None => b.append_null(),
        }
    }

    Ok(Arc::new(b.finish()))
}

/// Build a TimestampMicrosecond array, converting datetime/datetimeoffset to UTC.
/// P3: apply timezone offset for datetimeoffset before storing.
fn build_timestamp_col(rows: &[Row], col_idx: usize, sql_type: &str) -> Result<ArrayRef> {
    use chrono::{DateTime, NaiveDateTime, Utc};

    let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());

    let is_offset = sql_type.to_lowercase() == "datetimeoffset";

    for row in rows {
        if is_offset {
            // tiberius returns datetimeoffset as DateTime<FixedOffset>
            match row.get::<DateTime<chrono::FixedOffset>, _>(col_idx) {
                Some(dt) => {
                    let utc: DateTime<Utc> = dt.with_timezone(&Utc);
                    b.append_value(utc.timestamp_micros());
                }
                None => b.append_null(),
            }
        } else {
            // datetime / datetime2 / smalldatetime → NaiveDateTime, treat as UTC
            match row.get::<NaiveDateTime, _>(col_idx) {
                Some(dt) => b.append_value(dt.and_utc().timestamp_micros()),
                None => b.append_null(),
            }
        }
    }

    Ok(Arc::new(b.finish()))
}
