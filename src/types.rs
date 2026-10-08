use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::schema::ColumnInfo;

/// Map a SQL Server column type string → Arrow DataType.
pub fn sql_to_arrow(type_name: &str, precision: u8, scale: u8, _max_length: i16) -> DataType {
    match type_name.to_lowercase().as_str() {
        "bigint"                          => DataType::Int64,
        "int"                             => DataType::Int32,
        "smallint"                        => DataType::Int16,
        // SQL Server TINYINT is unsigned 0-255; use UInt8, not Int8.
        "tinyint"                         => DataType::UInt8,
        "bit"                             => DataType::Boolean,
        "float"                           => DataType::Float64,
        "real"                            => DataType::Float32,
        "decimal" | "numeric"             => DataType::Decimal128(precision, scale as i8),
        "money"                           => DataType::Decimal128(19, 4),
        "smallmoney"                      => DataType::Decimal128(10, 4),
        "char" | "varchar" | "text"       => DataType::Utf8,
        "nchar" | "nvarchar" | "ntext"    => DataType::Utf8,
        "binary" | "varbinary" | "image"  => DataType::Binary,
        // timestamp/rowversion is a 8-byte auto-generated value, not a real datetime
        "timestamp" | "rowversion"        => DataType::Binary,
        "datetime" | "smalldatetime"      => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "datetime2"                       => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "datetimeoffset"                  => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "date"                            => DataType::Date32,
        "time"                            => DataType::Time64(TimeUnit::Microsecond),
        "uniqueidentifier"                => DataType::FixedSizeBinary(16),
        "xml" | "json"                    => DataType::Utf8,
        // Spatial types: serialize as WKB
        "geography" | "geometry"          => DataType::Binary,
        // Unknown types: serialize as string via CAST in SELECT
        _                                 => DataType::Utf8,
    }
}

/// Build an Arrow Schema from column metadata.
pub fn build_arrow_schema(columns: &[ColumnInfo]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| {
            Field::new(
                &c.name,
                sql_to_arrow(&c.sql_type, c.precision, c.scale, c.max_length),
                c.is_nullable,
            )
        })
        .collect();
    Schema::new(fields)
}

/// Returns an override SELECT expression for types that need a CAST to be
/// readable by tiberius without a custom codec. Returns None if direct read is fine.
pub fn select_expr(col_name: &str, type_name: &str) -> Option<String> {
    let n = format!("[{col_name}]");
    match type_name.to_lowercase().as_str() {
        "xml"       => Some(format!("CAST({n} AS NVARCHAR(MAX))")),
        "geography" => Some(format!("{n}.STAsBinary()")),
        "geometry"  => Some(format!("{n}.STAsBinary()")),
        // timestamp/rowversion: cast to binary so tiberius returns raw bytes
        "timestamp" | "rowversion" => Some(format!("CAST({n} AS VARBINARY(8))")),
        _           => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_types() {
        assert_eq!(sql_to_arrow("int", 10, 0, 4), DataType::Int32);
        assert_eq!(sql_to_arrow("bigint", 19, 0, 8), DataType::Int64);
        assert_eq!(sql_to_arrow("smallint", 5, 0, 2), DataType::Int16);
        assert_eq!(sql_to_arrow("tinyint", 3, 0, 1), DataType::UInt8);
    }

    #[test]
    fn test_decimal_type() {
        assert_eq!(
            sql_to_arrow("decimal", 18, 4, -1),
            DataType::Decimal128(18, 4)
        );
    }

    #[test]
    fn test_string_types() {
        assert_eq!(sql_to_arrow("varchar", 0, 0, 100), DataType::Utf8);
        assert_eq!(sql_to_arrow("nvarchar", 0, 0, 200), DataType::Utf8);
    }

    #[test]
    fn test_select_expr_xml() {
        assert!(select_expr("MyCol", "xml").unwrap().contains("NVARCHAR(MAX)"));
    }

    #[test]
    fn test_select_expr_passthrough() {
        assert!(select_expr("MyCol", "int").is_none());
    }

    // ── Comprehensive sql_to_arrow coverage ──────────────────────────────────

    #[test]
    fn tinyint_maps_to_uint8() {
        assert_eq!(sql_to_arrow("tinyint", 3, 0, 1), DataType::UInt8);
    }

    #[test]
    fn bit_maps_to_boolean() {
        assert_eq!(sql_to_arrow("bit", 1, 0, 1), DataType::Boolean);
    }

    #[test]
    fn float_maps_to_float64() {
        assert_eq!(sql_to_arrow("float", 53, 0, 8), DataType::Float64);
    }

    #[test]
    fn real_maps_to_float32() {
        assert_eq!(sql_to_arrow("real", 24, 0, 4), DataType::Float32);
    }

    #[test]
    fn numeric_maps_to_decimal128() {
        assert_eq!(sql_to_arrow("numeric", 10, 2, -1), DataType::Decimal128(10, 2));
    }

    #[test]
    fn money_maps_to_decimal128_19_4() {
        assert_eq!(sql_to_arrow("money", 19, 4, 8), DataType::Decimal128(19, 4));
    }

    #[test]
    fn smallmoney_maps_to_decimal128_10_4() {
        assert_eq!(sql_to_arrow("smallmoney", 10, 4, 4), DataType::Decimal128(10, 4));
    }

    #[test]
    fn char_maps_to_utf8() {
        assert_eq!(sql_to_arrow("char", 0, 0, 10), DataType::Utf8);
    }

    #[test]
    fn text_maps_to_utf8() {
        assert_eq!(sql_to_arrow("text", 0, 0, -1), DataType::Utf8);
    }

    #[test]
    fn nchar_maps_to_utf8() {
        assert_eq!(sql_to_arrow("nchar", 0, 0, 20), DataType::Utf8);
    }

    #[test]
    fn ntext_maps_to_utf8() {
        assert_eq!(sql_to_arrow("ntext", 0, 0, -1), DataType::Utf8);
    }

    #[test]
    fn binary_maps_to_binary() {
        assert_eq!(sql_to_arrow("binary", 0, 0, 8), DataType::Binary);
    }

    #[test]
    fn varbinary_maps_to_binary() {
        assert_eq!(sql_to_arrow("varbinary", 0, 0, -1), DataType::Binary);
    }

    #[test]
    fn image_maps_to_binary() {
        assert_eq!(sql_to_arrow("image", 0, 0, -1), DataType::Binary);
    }

    #[test]
    fn timestamp_maps_to_binary() {
        assert_eq!(sql_to_arrow("timestamp", 0, 0, 8), DataType::Binary);
    }

    #[test]
    fn rowversion_maps_to_binary() {
        assert_eq!(sql_to_arrow("rowversion", 0, 0, 8), DataType::Binary);
    }

    #[test]
    fn datetime_maps_to_timestamp_utc() {
        assert_eq!(
            sql_to_arrow("datetime", 23, 3, 8),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn smalldatetime_maps_to_timestamp_utc() {
        assert_eq!(
            sql_to_arrow("smalldatetime", 16, 0, 4),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn datetime2_maps_to_timestamp_utc() {
        assert_eq!(
            sql_to_arrow("datetime2", 27, 7, 8),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn datetimeoffset_maps_to_timestamp_utc() {
        assert_eq!(
            sql_to_arrow("datetimeoffset", 34, 7, 10),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn date_maps_to_date32() {
        assert_eq!(sql_to_arrow("date", 10, 0, 3), DataType::Date32);
    }

    #[test]
    fn time_maps_to_time64_microsecond() {
        assert_eq!(sql_to_arrow("time", 16, 7, 5), DataType::Time64(TimeUnit::Microsecond));
    }

    #[test]
    fn uniqueidentifier_maps_to_fixed_size_binary_16() {
        assert_eq!(sql_to_arrow("uniqueidentifier", 0, 0, 16), DataType::FixedSizeBinary(16));
    }

    #[test]
    fn xml_maps_to_utf8() {
        assert_eq!(sql_to_arrow("xml", 0, 0, -1), DataType::Utf8);
    }

    #[test]
    fn json_maps_to_utf8() {
        assert_eq!(sql_to_arrow("json", 0, 0, -1), DataType::Utf8);
    }

    #[test]
    fn geography_maps_to_binary() {
        assert_eq!(sql_to_arrow("geography", 0, 0, -1), DataType::Binary);
    }

    #[test]
    fn geometry_maps_to_binary() {
        assert_eq!(sql_to_arrow("geometry", 0, 0, -1), DataType::Binary);
    }

    #[test]
    fn unknown_type_maps_to_utf8() {
        assert_eq!(sql_to_arrow("hierarchyid", 0, 0, -1), DataType::Utf8);
        assert_eq!(sql_to_arrow("sysname", 0, 0, -1), DataType::Utf8);
    }

    #[test]
    fn case_insensitive_mapping() {
        assert_eq!(sql_to_arrow("INT", 10, 0, 4), DataType::Int32);
        assert_eq!(sql_to_arrow("BigInt", 19, 0, 8), DataType::Int64);
        assert_eq!(sql_to_arrow("DATETIME2", 27, 7, 8),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
    }

    // ── select_expr overrides ─────────────────────────────────────────────────

    #[test]
    fn select_expr_xml_casts_to_nvarchar_max() {
        let expr = select_expr("XmlData", "xml").expect("xml should have an override");
        assert!(expr.contains("NVARCHAR(MAX)"), "got: {expr}");
        assert!(expr.contains("[XmlData]"), "got: {expr}");
    }

    #[test]
    fn select_expr_geography_uses_stasbinaary() {
        let expr = select_expr("Location", "geography").expect("geography should have an override");
        assert!(expr.contains("STAsBinary()"), "got: {expr}");
        assert!(expr.contains("[Location]"), "got: {expr}");
    }

    #[test]
    fn select_expr_geometry_uses_stasbinaary() {
        let expr = select_expr("Shape", "geometry").expect("geometry should have an override");
        assert!(expr.contains("STAsBinary()"), "got: {expr}");
        assert!(expr.contains("[Shape]"), "got: {expr}");
    }

    #[test]
    fn select_expr_timestamp_casts_to_varbinary() {
        let expr = select_expr("RowVer", "timestamp").expect("timestamp should have an override");
        assert!(expr.contains("VARBINARY(8)"), "got: {expr}");
        assert!(expr.contains("[RowVer]"), "got: {expr}");
    }

    #[test]
    fn select_expr_rowversion_casts_to_varbinary() {
        let expr = select_expr("RV", "rowversion").expect("rowversion should have an override");
        assert!(expr.contains("VARBINARY(8)"), "got: {expr}");
        assert!(expr.contains("[RV]"), "got: {expr}");
    }

    // ── select_expr passthrough for no-override types ─────────────────────────

    #[test]
    fn select_expr_passthrough_int() {
        assert!(select_expr("MyCol", "int").is_none());
    }

    #[test]
    fn select_expr_passthrough_bigint() {
        assert!(select_expr("ID", "bigint").is_none());
    }

    #[test]
    fn select_expr_passthrough_varchar() {
        assert!(select_expr("Name", "varchar").is_none());
    }

    #[test]
    fn select_expr_passthrough_nvarchar() {
        assert!(select_expr("Title", "nvarchar").is_none());
    }

    #[test]
    fn select_expr_passthrough_datetime2() {
        assert!(select_expr("CreatedAt", "datetime2").is_none());
    }

    #[test]
    fn select_expr_passthrough_decimal() {
        assert!(select_expr("Amount", "decimal").is_none());
    }

    #[test]
    fn select_expr_passthrough_uniqueidentifier() {
        assert!(select_expr("RowGuid", "uniqueidentifier").is_none());
    }

    #[test]
    fn select_expr_passthrough_bit() {
        assert!(select_expr("IsActive", "bit").is_none());
    }

    #[test]
    fn select_expr_passthrough_varbinary() {
        assert!(select_expr("Data", "varbinary").is_none());
    }
}
