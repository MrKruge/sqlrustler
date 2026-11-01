use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Timelike;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::path::Path;
use std::sync::Arc;
use tiberius::Row;

use crate::schema::{ColumnInfo, TableInfo};
use crate::types::build_arrow_schema;

/// Pipelined writer that runs on a blocking thread, receiving row batches via channel.
/// This decouples the async TDS reader from the CPU-bound Arrow conversion + Parquet compression.
pub struct BlockingParquetWriter {
    receiver: std::sync::mpsc::Receiver<Vec<Row>>,
    writer: ArrowWriter<std::fs::File>,
    schema: Arc<Schema>,
    columns: Vec<ColumnInfo>,
}

impl BlockingParquetWriter {
    pub fn new(
        path: &Path,
        table: &TableInfo,
        compression_level: i32,
        receiver: std::sync::mpsc::Receiver<Vec<Row>>,
    ) -> Result<Self> {
        let schema = Arc::new(build_arrow_schema(&table.columns));
        let file = std::fs::File::create(path)
            .with_context(|| format!("Failed to create parquet file: {}", path.display()))?;

        let zstd = ZstdLevel::try_new(compression_level.clamp(1, 22))?;
        // Set row group size to match batch_size so the ArrowWriter flushes incrementally
        // rather than buffering the entire chunk (up to ~1M rows default) before close().
        // This keeps peak memory per chunk bounded at ~batch_size rows.
        // BlockingParquetWriter receives batches via channel so batch_size is the natural boundary.
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(zstd))
            .set_max_row_group_size(100_000)
            .build();

        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .context("Failed to create ArrowWriter")?;

        Ok(Self {
            receiver,
            writer,
            schema,
            columns: table.columns.clone(),
        })
    }

    /// Drain channel until closed, converting each batch to Parquet. Returns total rows written.
    pub fn run(mut self) -> Result<u64> {
        let mut rows_written = 0u64;
        while let Ok(rows) = self.receiver.recv() {
            let batch = rows_to_record_batch(&rows, &self.columns, self.schema.clone())
                .context("Failed to convert rows to RecordBatch")?;
            rows_written += batch.num_rows() as u64;
            self.writer
                .write(&batch)
                .context("Failed to write RecordBatch")?;
        }
        self.writer.close().context("Failed to close ArrowWriter")?;
        Ok(rows_written)
    }
}

// ── Core conversion: Vec<Row> → RecordBatch ───────────────────────────────────

/// Read one cell. Uses `try_get` rather than `Row::get` (which `unwrap`s): a driver/schema
/// type mismatch must surface as an error naming the column instead of a bare panic that
/// takes down the writer thread.
fn cell<'a, T: tiberius::FromSql<'a>>(
    row: &'a Row,
    idx: usize,
    col: &ColumnInfo,
) -> Result<Option<T>> {
    row.try_get::<T, _>(idx).map_err(|e| {
        anyhow::anyhow!(
            "column [{}] (SQL type {}): cannot read value as {}: {e}",
            col.name,
            col.sql_type,
            std::any::type_name::<T>()
        )
    })
}

/// Macro to build a primitive Arrow array from tiberius rows.
/// Usage: build_primitive_col!(rows, col_idx, col_info, i32, Int32Builder)
macro_rules! build_primitive_col {
    ($rows:expr, $idx:expr, $col:expr, $rust_type:ty, $builder:ty) => {{
        let mut b = <$builder>::with_capacity($rows.len());
        for row in $rows {
            match cell::<$rust_type>(row, $idx, $col)? {
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
            DataType::Int64 => build_primitive_col!(rows, col_idx, col_info, i64, Int64Builder),
            DataType::Int32 => build_primitive_col!(rows, col_idx, col_info, i32, Int32Builder),
            DataType::Int16 => build_primitive_col!(rows, col_idx, col_info, i16, Int16Builder),
            DataType::UInt8 => {
                // SQL Server TINYINT is unsigned 0-255; tiberius returns u8
                let mut b = UInt8Builder::with_capacity(rows.len());
                for row in rows {
                    match cell::<u8>(row, col_idx, col_info)? {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Boolean => {
                build_primitive_col!(rows, col_idx, col_info, bool, BooleanBuilder)
            }
            DataType::Float64 => build_primitive_col!(rows, col_idx, col_info, f64, Float64Builder),
            DataType::Float32 => build_primitive_col!(rows, col_idx, col_info, f32, Float32Builder),

            DataType::Decimal128(precision, scale) => {
                build_decimal128_col(rows, col_idx, col_info, *precision, *scale)?
            }

            DataType::Utf8 => {
                // Size the value buffer from the column's max_length metadata.
                // max_length == -1 means MAX (nvarchar(max)/varchar(max)/text) —
                // assume 500 bytes avg to avoid constant reallocation on LOB columns.
                let avg_bytes = if col_info.max_length < 0 {
                    500
                } else {
                    col_info.max_length.max(8) as usize
                };
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * avg_bytes);
                for row in rows {
                    match cell::<&str>(row, col_idx, col_info)? {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }

            DataType::Binary => {
                let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows {
                    match cell::<&[u8]>(row, col_idx, col_info)? {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }

            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                build_timestamp_col(rows, col_idx, col_info)?
            }

            DataType::Date32 => {
                use chrono::NaiveDate;
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let mut b = Date32Builder::with_capacity(rows.len());
                for row in rows {
                    match cell::<NaiveDate>(row, col_idx, col_info)? {
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
                    match cell::<NaiveTime>(row, col_idx, col_info)? {
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
                    match cell::<uuid::Uuid>(row, col_idx, col_info)? {
                        Some(v) => b.append_value(v.as_bytes())?,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }

            // Fallback for unknown types
            _ => {
                let avg_bytes = if col_info.max_length < 0 {
                    500
                } else {
                    col_info.max_length.max(8) as usize
                };
                let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * avg_bytes);
                for row in rows {
                    match cell::<&str>(row, col_idx, col_info)? {
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
    col_info: &ColumnInfo,
    precision: u8,
    scale: i8,
) -> Result<ArrayRef> {
    let mut b =
        Decimal128Builder::with_capacity(rows.len()).with_precision_and_scale(precision, scale)?;

    for row in rows {
        // tiberius 0.12 with bigdecimal 0.3: NUMERIC comes back as tiberius::numeric::Numeric
        // which implements FromSql. We use i128 via tiberius's own Numeric type.
        match cell::<tiberius::numeric::Numeric>(row, col_idx, col_info)? {
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
fn build_timestamp_col(rows: &[Row], col_idx: usize, col_info: &ColumnInfo) -> Result<ArrayRef> {
    use chrono::{DateTime, NaiveDateTime, Utc};

    let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());

    let is_offset = col_info.sql_type.eq_ignore_ascii_case("datetimeoffset");

    for row in rows {
        if is_offset {
            // tiberius returns datetimeoffset as DateTime<FixedOffset>
            match cell::<DateTime<chrono::FixedOffset>>(row, col_idx, col_info)? {
                Some(dt) => {
                    let utc: DateTime<Utc> = dt.with_timezone(&Utc);
                    b.append_value(utc.timestamp_micros());
                }
                None => b.append_null(),
            }
        } else {
            // datetime / datetime2 / smalldatetime → NaiveDateTime, treat as UTC
            match cell::<NaiveDateTime>(row, col_idx, col_info)? {
                Some(dt) => b.append_value(dt.and_utc().timestamp_micros()),
                None => b.append_null(),
            }
        }
    }

    Ok(Arc::new(
        b.finish().with_timezone_opt(Some(Arc::from("UTC"))),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiberius::numeric::Numeric;
    use tiberius::time::{Date, DateTime2, DateTimeOffset, Time};
    use tiberius::{ColumnData, ColumnType, RowBuilder};

    /// Days from 0001-01-01 (SQL Server `date` epoch) to 1970-01-01.
    const DAYS_TO_UNIX_EPOCH: u32 = 719_162;

    fn col(name: &str, ty: &str, precision: u8, scale: u8, max_length: i16) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            sql_type: ty.into(),
            precision,
            scale,
            max_length,
            is_nullable: true,
            is_identity: false,
            identity_seed: None,
            identity_increment: None,
        }
    }

    fn one_col_batch(
        c: ColumnInfo,
        ct: ColumnType,
        values: Vec<ColumnData<'static>>,
    ) -> Result<RecordBatch> {
        let cols = vec![c];
        let schema = Arc::new(build_arrow_schema(&cols));
        let rows: Vec<Row> = values
            .into_iter()
            .map(|v| {
                RowBuilder::default()
                    .column(cols[0].name.clone(), ct, v)
                    .build()
            })
            .collect();
        rows_to_record_batch(&rows, &cols, schema)
    }

    #[test]
    fn decimal_keeps_unscaled_value_precision_and_scale() {
        let c = col("d", "decimal", 38, 10, 17);
        let big: i128 = -12_345_678_901_234_567_890_123_456_789_i128;
        let b = one_col_batch(
            c,
            ColumnType::Decimaln,
            vec![
                ColumnData::Numeric(Some(Numeric::new_with_scale(big, 10))),
                ColumnData::Numeric(None),
            ],
        )
        .unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!((a.precision(), a.scale()), (38, 10));
        assert_eq!(a.value(0), big);
        assert!(a.is_null(1));
    }

    #[test]
    fn money_selected_as_decimal_round_trips_exact_cents() {
        // select_expr casts MONEY to DECIMAL(19,4), so the wire value is a Numeric. A value
        // above 2^53 cents cannot survive the f64 that tiberius uses for native MONEY.
        let c = col("m", "money", 19, 4, 8);
        let max_money_unscaled: i128 = 9_223_372_036_854_775_807; // 922337203685477.5807
        let b = one_col_batch(
            c,
            ColumnType::Decimaln,
            vec![ColumnData::Numeric(Some(Numeric::new_with_scale(
                max_money_unscaled,
                4,
            )))],
        )
        .unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(a.value(0), max_money_unscaled);
        assert_eq!((a.precision(), a.scale()), (19, 4));
    }

    #[test]
    fn type_mismatch_is_an_error_naming_the_column_not_a_panic() {
        // Native MONEY arrives as F64; reading it as Numeric used to hit Row::get's unwrap.
        let c = col("Price", "decimal", 19, 4, 9);
        let err = one_col_batch(c, ColumnType::Money, vec![ColumnData::F64(Some(1.5))])
            .expect_err("mismatch must be an error");
        let msg = format!("{err:#}");
        assert!(msg.contains("[Price]"), "{msg}");
    }

    #[test]
    fn nulls_are_preserved_for_every_type() {
        let cases: Vec<(ColumnInfo, ColumnData<'static>)> = vec![
            (col("a", "bigint", 19, 0, 8), ColumnData::I64(None)),
            (col("a", "int", 10, 0, 4), ColumnData::I32(None)),
            (col("a", "smallint", 5, 0, 2), ColumnData::I16(None)),
            (col("a", "tinyint", 3, 0, 1), ColumnData::U8(None)),
            (col("a", "bit", 1, 0, 1), ColumnData::Bit(None)),
            (col("a", "float", 53, 0, 8), ColumnData::F64(None)),
            (col("a", "real", 24, 0, 4), ColumnData::F32(None)),
            (col("a", "nvarchar", 0, 0, -1), ColumnData::String(None)),
            (col("a", "varbinary", 0, 0, -1), ColumnData::Binary(None)),
            (
                col("a", "uniqueidentifier", 0, 0, 16),
                ColumnData::Guid(None),
            ),
            (col("a", "date", 10, 0, 3), ColumnData::Date(None)),
            (col("a", "time", 16, 7, 5), ColumnData::Time(None)),
            (col("a", "datetime2", 27, 7, 8), ColumnData::DateTime2(None)),
            (
                col("a", "datetimeoffset", 34, 7, 10),
                ColumnData::DateTimeOffset(None),
            ),
        ];
        for (c, v) in cases {
            let name = c.sql_type.clone();
            let b = one_col_batch(c, ColumnType::Null, vec![v])
                .unwrap_or_else(|e| panic!("{name}: {e:#}"));
            assert_eq!(b.num_rows(), 1, "{name}");
            assert!(b.column(0).is_null(0), "{name} NULL lost");
        }
    }

    #[test]
    fn nvarchar_max_and_varbinary_max_round_trip_large_values() {
        let text: String = "é😀日本".repeat(100_000); // multi-byte, ~1 MB
        let bytes: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
        let b = one_col_batch(
            col("t", "nvarchar", 0, 0, -1),
            ColumnType::NVarchar,
            vec![
                ColumnData::String(Some(text.clone().into())),
                ColumnData::String(Some("".into())),
            ],
        )
        .unwrap();
        let a = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.value(0), text);
        assert_eq!(a.value(1), "", "empty string must stay empty, not NULL");
        assert!(!a.is_null(1));

        let b = one_col_batch(
            col("b", "varbinary", 0, 0, -1),
            ColumnType::BigVarBin,
            vec![
                ColumnData::Binary(Some(bytes.clone().into())),
                ColumnData::Binary(Some(Vec::<u8>::new().into())),
            ],
        )
        .unwrap();
        let a = b.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(a.value(0), &bytes[..]);
        assert!(!a.is_null(1) && a.value(1).is_empty());
    }

    #[test]
    fn rowversion_bytes_are_kept_verbatim() {
        let rv = vec![0u8, 0, 0, 0, 0, 0, 0x0B, 0xB8];
        let b = one_col_batch(
            col("rv", "timestamp", 0, 0, 8),
            ColumnType::BigVarBin,
            vec![ColumnData::Binary(Some(rv.clone().into()))],
        )
        .unwrap();
        let a = b.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(a.value(0), &rv[..]);
    }

    #[test]
    fn uniqueidentifier_is_stored_in_canonical_text_order() {
        // tiberius already un-swaps the mixed-endian wire bytes, so the Uuid's bytes are the
        // canonical (RFC 4122 / text) order. The archive therefore holds canonical bytes.
        let u = uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let b = one_col_batch(
            col("g", "uniqueidentifier", 0, 0, 16),
            ColumnType::Guid,
            vec![ColumnData::Guid(Some(u))],
        )
        .unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(
            a.value(0),
            &[
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff
            ]
        );
    }

    #[test]
    fn datetime2_keeps_microseconds_and_truncates_sub_micro_digits() {
        // 2024-03-05 12:34:56.1234567 (scale 7, 100 ns increments)
        let days = DAYS_TO_UNIX_EPOCH + 19_787; // 2024-03-05
        let secs_of_day: u64 = 12 * 3600 + 34 * 60 + 56;
        let incr = secs_of_day * 10_000_000 + 1_234_567;
        let b = one_col_batch(
            col("d", "datetime2", 27, 7, 8),
            ColumnType::Datetime2,
            vec![ColumnData::DateTime2(Some(DateTime2::new(
                Date::new(days),
                Time::new(incr, 7),
            )))],
        )
        .unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected = chrono::NaiveDate::from_ymd_opt(2024, 3, 5)
            .unwrap()
            .and_hms_micro_opt(12, 34, 56, 123_456)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(a.value(0), expected);
        assert_eq!(
            b.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn datetimeoffset_is_stored_as_the_utc_instant() {
        // TDS carries datetimeoffset date/time as UTC plus an informational offset (+05:30 here);
        // the stored Arrow value must be that UTC instant, unshifted by the offset.
        let days = DAYS_TO_UNIX_EPOCH + 19_787; // 2024-03-05
        let incr = 12u64 * 3600 * 10_000_000;
        let dto = DateTimeOffset::new(DateTime2::new(Date::new(days), Time::new(incr, 7)), 330);
        let b = one_col_batch(
            col("o", "datetimeoffset", 34, 7, 10),
            ColumnType::DatetimeOffsetn,
            vec![ColumnData::DateTimeOffset(Some(dto))],
        )
        .unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let expected = chrono::NaiveDate::from_ymd_opt(2024, 3, 5)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        assert_eq!(a.value(0), expected);
    }

    #[test]
    fn date_and_time_values_and_pre_epoch_dates() {
        let b = one_col_batch(
            col("d", "date", 10, 0, 3),
            ColumnType::Daten,
            vec![
                ColumnData::Date(Some(Date::new(DAYS_TO_UNIX_EPOCH))), // 1970-01-01
                ColumnData::Date(Some(Date::new(DAYS_TO_UNIX_EPOCH - 1))), // 1969-12-31
                ColumnData::Date(Some(Date::new(0))),                  // 0001-01-01
            ],
        )
        .unwrap();
        let a = b.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(a.value(0), 0);
        assert_eq!(a.value(1), -1);
        assert_eq!(a.value(2), -(DAYS_TO_UNIX_EPOCH as i32));

        // time(7) 23:59:59.9999999 -> 86_399_999_999 us (100 ns digit truncated)
        let b = one_col_batch(
            col("t", "time", 16, 7, 5),
            ColumnType::Timen,
            vec![ColumnData::Time(Some(Time::new(863_999_999_999, 7)))],
        )
        .unwrap();
        let a = b
            .column(0)
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(a.value(0), 86_399_999_999);
    }

    #[test]
    fn tinyint_and_integer_extremes() {
        let b = one_col_batch(
            col("t", "tinyint", 3, 0, 1),
            ColumnType::Int1,
            vec![ColumnData::U8(Some(0)), ColumnData::U8(Some(255))],
        )
        .unwrap();
        let a = b.column(0).as_any().downcast_ref::<UInt8Array>().unwrap();
        assert_eq!((a.value(0), a.value(1)), (0, 255));

        let b = one_col_batch(
            col("i", "bigint", 19, 0, 8),
            ColumnType::Int8,
            vec![
                ColumnData::I64(Some(i64::MIN)),
                ColumnData::I64(Some(i64::MAX)),
            ],
        )
        .unwrap();
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!((a.value(0), a.value(1)), (i64::MIN, i64::MAX));
    }
}
