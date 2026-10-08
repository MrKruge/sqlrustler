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
}
