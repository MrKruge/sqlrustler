//! Parquet round-trip + type-mapping tests using only the public API. No DB.
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sqlrustler::archive::{ArchiveReader, ArchiveWriter};
use sqlrustler::schema::{ColumnInfo, POST_DATA_MARKER};
use sqlrustler::types::{build_arrow_schema, select_expr, sql_to_arrow};
use std::sync::Arc;

fn col(name: &str, ty: &str, p: u8, s: u8, ml: i16, nullable: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.into(),
        sql_type: ty.into(),
        precision: p,
        scale: s,
        max_length: ml,
        is_nullable: nullable,
        is_identity: false,
        identity_seed: None,
        identity_increment: None,
    }
}

fn write_parquet(
    batches: &[RecordBatch],
    schema: arrow::datatypes::SchemaRef,
    rg: usize,
) -> Vec<u8> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_size(rg)
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    for b in batches {
        w.write(b).unwrap();
    }
    w.close().unwrap();
    buf
}

fn read_parquet(bytes: Vec<u8>) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap()
        .build()
        .unwrap()
        .map(|b| b.unwrap())
        .collect()
}

fn all_types_columns() -> Vec<ColumnInfo> {
    vec![
        col("i", "int", 10, 0, 4, true),
        col("bi", "bigint", 19, 0, 8, true),
        col("si", "smallint", 5, 0, 2, true),
        col("ti", "tinyint", 3, 0, 1, true),
        col("b", "bit", 1, 0, 1, true),
        col("f", "float", 53, 0, 8, true),
        col("r", "real", 24, 0, 4, true),
        col("d", "decimal", 18, 4, 9, true),
        col("nvc", "nvarchar", 0, 0, -1, true),
        col("bin", "varbinary", 0, 0, -1, true),
        col("dt", "datetime2", 27, 7, 8, true),
        col("g", "uniqueidentifier", 0, 0, 16, true),
        col("dte", "date", 10, 0, 3, true),
        col("tm", "time", 16, 7, 5, true),
    ]
}

#[test]
fn type_mapping_edge_cases() {
    assert_eq!(
        sql_to_arrow("INT", 0, 0, 4),
        DataType::Int32,
        "case-insensitive"
    );
    assert_eq!(sql_to_arrow("tinyint", 0, 0, 1), DataType::UInt8);
    assert_eq!(
        sql_to_arrow("decimal", 38, 38, 17),
        DataType::Decimal128(38, 38)
    );
    assert_eq!(sql_to_arrow("money", 0, 0, 8), DataType::Decimal128(19, 4));
    assert_eq!(
        sql_to_arrow("smallmoney", 0, 0, 4),
        DataType::Decimal128(10, 4)
    );
    assert_eq!(
        sql_to_arrow("uniqueidentifier", 0, 0, 16),
        DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        sql_to_arrow("some_clr_udt", 0, 0, -1),
        DataType::Utf8,
        "unknown -> Utf8 fallback"
    );
    assert_eq!(sql_to_arrow("", 0, 0, 0), DataType::Utf8);
    assert_eq!(sql_to_arrow("rowversion", 0, 0, 8), DataType::Binary);
    assert_eq!(
        sql_to_arrow("time", 0, 0, 5),
        DataType::Time64(TimeUnit::Microsecond)
    );
}

#[test]
fn every_special_type_has_select_expr_override() {
    for t in [
        "xml",
        "sql_variant",
        "hierarchyid",
        "geography",
        "geometry",
        "timestamp",
        "rowversion",
    ] {
        assert!(
            select_expr("c", t).is_some(),
            "{t} needs CAST/ToString override"
        );
    }
    for t in ["int", "nvarchar", "datetime2", "uniqueidentifier"] {
        assert!(select_expr("c", t).is_none(), "{t}");
    }
}

#[test]
fn select_expr_brackets_column_name() {
    assert!(select_expr("My Col", "xml").unwrap().contains("[My Col]"));
}

#[test]
fn select_expr_column_name_with_closing_bracket_is_escaped() {
    // A column literally named "a]b" must be emitted as [a]]b] or the SELECT is malformed / injectable.
    let e = select_expr("a]b", "xml").unwrap();
    assert!(e.contains("[a]]b]"), "unescaped ] in identifier: {e}");
}

#[test]
fn schema_preserves_names_order_and_nullability() {
    let cols = vec![
        col("z", "int", 10, 0, 4, false),
        col("a", "nvarchar", 0, 0, 20, true),
    ];
    let s = build_arrow_schema(&cols);
    assert_eq!(s.fields().len(), 2);
    assert_eq!(s.field(0).name(), "z");
    assert!(!s.field(0).is_nullable());
    assert!(s.field(1).is_nullable());
}

#[test]
fn schema_with_zero_columns_is_empty() {
    assert_eq!(build_arrow_schema(&[]).fields().len(), 0);
}

#[test]
fn unicode_and_duplicate_ish_column_names_in_schema() {
    let s = build_arrow_schema(&[
        col("名前", "nvarchar", 0, 0, 10, true),
        col("😀", "int", 10, 0, 4, true),
    ]);
    assert_eq!(s.field(0).name(), "名前");
    assert_eq!(s.field(1).name(), "😀");
}

#[test]
fn all_types_batch_with_nulls_round_trips_through_parquet() {
    let schema = Arc::new(build_arrow_schema(&all_types_columns()));
    let dec = Decimal128Array::from(vec![Some(12345678i128), None, Some(-12345678)])
        .with_precision_and_scale(18, 4)
        .unwrap();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)])),
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])),
        Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])),
        Arc::new(UInt8Array::from(vec![Some(0u8), None, Some(255)])),
        Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
        Arc::new(Float64Array::from(vec![Some(f64::MAX), None, Some(-0.0)])),
        Arc::new(Float32Array::from(vec![
            Some(f32::MIN_POSITIVE),
            None,
            Some(1.5),
        ])),
        Arc::new(dec),
        Arc::new(StringArray::from(vec![
            Some("héllo 😀 日本語"),
            None,
            Some(""),
        ])),
        Arc::new(BinaryArray::from_opt_vec(vec![
            Some(&[0u8, 255, 1][..]),
            None,
            Some(&[][..]),
        ])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![Some(0i64), None, Some(253402300799999999)])
                .with_timezone("UTC"),
        ),
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![Some(vec![1u8; 16]), None, Some(vec![255u8; 16])].into_iter(),
                16,
            )
            .unwrap(),
        ),
        Arc::new(Date32Array::from(vec![Some(-719162), None, Some(2932896)])), // 0001-01-01 .. 9999-12-31
        Arc::new(Time64MicrosecondArray::from(vec![
            Some(0i64),
            None,
            Some(86_399_999_999),
        ])),
    ];
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let out = read_parquet(write_parquet(std::slice::from_ref(&batch), schema, 100_000));
    assert_eq!(out.len(), 1);
    assert_eq!(
        out[0], batch,
        "parquet round-trip must be lossless for every mapped type"
    );
}

#[test]
fn all_null_columns_round_trip() {
    let schema = Arc::new(build_arrow_schema(&[
        col("a", "int", 10, 0, 4, true),
        col("b", "nvarchar", 0, 0, 10, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![None, None, None])),
            Arc::new(StringArray::from(vec![None::<&str>, None, None])),
        ],
    )
    .unwrap();
    let out = read_parquet(write_parquet(std::slice::from_ref(&batch), schema, 10));
    assert_eq!(out[0], batch);
    assert_eq!(out[0].column(0).null_count(), 3);
}

#[test]
fn zero_row_table_produces_valid_parquet_with_schema() {
    let schema = Arc::new(build_arrow_schema(&all_types_columns()));
    let bytes = write_parquet(&[], schema.clone(), 1000);
    let b = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    assert_eq!(b.metadata().file_metadata().num_rows(), 0);
    assert_eq!(b.schema().fields().len(), schema.fields().len());
    assert_eq!(b.build().unwrap().count(), 0);
}

#[test]
fn zero_column_table_edge() {
    // Tables always have >=1 column in SQL Server; confirm an empty schema doesn't panic the writer.
    let schema = Arc::new(build_arrow_schema(&[]));
    let r = std::panic::catch_unwind(|| write_parquet(&[], schema, 10));
    assert!(r.is_ok());
}

#[test]
fn many_row_groups_preserve_order_and_count() {
    let schema = Arc::new(build_arrow_schema(&[col("id", "bigint", 19, 0, 8, false)]));
    let n = 25_000i64;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..n))],
    )
    .unwrap();
    let bytes = write_parquet(&[batch], schema, 1000);
    let b = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    assert_eq!(b.metadata().num_row_groups(), 25);
    let mut expect = 0i64;
    for rb in b.build().unwrap() {
        let rb = rb.unwrap();
        let a = rb.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for v in a.iter() {
            assert_eq!(v.unwrap(), expect);
            expect += 1;
        }
    }
    assert_eq!(expect, n);
}

#[test]
fn identity_gaps_values_preserved() {
    let schema = Arc::new(build_arrow_schema(&[col("Id", "int", 10, 0, 4, false)]));
    let ids = vec![1, 2, 5, 100, 1_000_000, i32::MAX];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(ids.clone()))],
    )
    .unwrap();
    let out = read_parquet(write_parquet(&[batch], schema, 10));
    let a = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(a.values().to_vec(), ids);
}

#[test]
fn decimal_extremes_round_trip() {
    let schema = Arc::new(build_arrow_schema(&[col(
        "d", "decimal", 38, 10, 17, false,
    )]));
    let max = 10i128.pow(38) - 1;
    let arr = Decimal128Array::from(vec![max, -max, 0, 1])
        .with_precision_and_scale(38, 10)
        .unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)]).unwrap();
    let out = read_parquet(write_parquet(std::slice::from_ref(&batch), schema, 10));
    assert_eq!(out[0], batch);
}

#[test]
fn max_length_lob_values_round_trip() {
    // 4 MiB NVARCHAR(MAX)-ish string and 4 MiB VARBINARY(MAX)
    let schema = Arc::new(build_arrow_schema(&[
        col("t", "nvarchar", 0, 0, -1, false),
        col("b", "varbinary", 0, 0, -1, false),
    ]));
    let s: String = "ü😀x".repeat(700_000);
    let bin: Vec<u8> = (0..4 * 1024 * 1024u32).map(|i| i as u8).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![s.as_str()])),
            Arc::new(BinaryArray::from_vec(vec![bin.as_slice()])),
        ],
    )
    .unwrap();
    let out = read_parquet(write_parquet(std::slice::from_ref(&batch), schema, 10));
    assert_eq!(out[0], batch);
}

#[test]
fn embedded_nul_and_control_chars_in_strings_survive() {
    let schema = Arc::new(build_arrow_schema(&[col("t", "nvarchar", 0, 0, 50, false)]));
    let vals = vec![
        "a\0b",
        "line1\r\nline2\t'quote\"",
        "\u{FEFF}bom",
        "\u{10FFFF}",
    ];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vals.clone()))],
    )
    .unwrap();
    let out = read_parquet(write_parquet(&[batch], schema, 10));
    let a = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for (i, v) in vals.iter().enumerate() {
        assert_eq!(a.value(i), *v);
    }
}

#[test]
fn truncated_parquet_bytes_error_not_panic() {
    let schema = Arc::new(build_arrow_schema(&[col("id", "bigint", 19, 0, 8, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(0..1000))],
    )
    .unwrap();
    let full = write_parquet(&[batch], schema, 100);
    for cut in [0, 3, 4, full.len() / 2, full.len() - 1] {
        let part = full[..cut].to_vec();
        let r = std::panic::catch_unwind(move || {
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(part)).map(|_| ())
        });
        assert!(r.is_ok(), "parquet reader panicked at cut={cut}");
        assert!(
            r.unwrap().is_err(),
            "truncated parquet accepted at cut={cut}"
        );
    }
}

#[test]
fn parquet_inside_archive_round_trips_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(build_arrow_schema(&all_types_columns()[..2]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![3, 4])),
        ],
    )
    .unwrap();
    let bytes = write_parquet(std::slice::from_ref(&batch), schema, 10);
    let pq = dir.path().join("t.parquet");
    std::fs::write(&pq, &bytes).unwrap();
    let ar = dir.path().join("a.rustler");
    let mut w = ArchiveWriter::create(&ar, 3).unwrap();
    w.add_parquet_file("data/dbo_T.parquet", &pq).unwrap();
    w.finish().unwrap();
    let got = ArchiveReader::open(&ar)
        .unwrap()
        .read_parquet("data/dbo_T.parquet")
        .unwrap();
    assert_eq!(got, bytes);
    assert_eq!(read_parquet(got)[0], batch);
}

#[test]
fn post_data_marker_is_a_sql_comment_line() {
    assert!(POST_DATA_MARKER.starts_with("--"));
    assert!(!POST_DATA_MARKER.contains('\n'));
}
