use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet, VecDeque};

use crate::connection::DbPool;

// ── Column / Table / FK structs ───────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub sql_type: String,
    pub precision: u8,
    pub scale: u8,
    pub max_length: i16,
    pub is_nullable: bool,
    pub is_identity: bool,
    pub identity_seed: Option<i64>,
    pub identity_increment: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    /// Fully-qualified bracketed name, e.g. "[dbo].[Orders]"
    pub fqn: String,
    pub columns: Vec<ColumnInfo>,
    pub identity_columns: Vec<String>,
    /// CREATE TABLE DDL (without trailing GO)
    pub create_ddl: String,
}

#[derive(Debug, Clone)]
pub struct FkInfo {
    pub name: String,
    pub parent_fqn: String,
    pub ref_fqn: String,
    /// Full ALTER TABLE ADD CONSTRAINT statement
    pub alter_stmt: String,
    /// ALTER TABLE DROP CONSTRAINT statement
    pub drop_stmt: String,
}

pub struct SchemaBundle {
    /// Tables in topological order (parents before children)
    pub tables: Vec<TableInfo>,
    pub fks: Vec<FkInfo>,
    /// Complete schema.sql content with POST_DATA marker
    pub schema_sql: String,
}

// ── Main extraction entry point ───────────────────────────────────────────────

pub async fn extract(pool: &DbPool, exclude_globs: &[String]) -> Result<SchemaBundle> {
    let mut conn = pool.get().await.context("Failed to get connection for schema extraction")?;

    // 1. Enumerate tables
    let all_tables = query_tables(&mut conn).await?;

    // 2. Apply exclusion filters
    let tables_filtered: Vec<_> = all_tables
        .into_iter()
        .filter(|(schema, name)| !is_excluded(schema, name, exclude_globs))
        .collect();

    // 3. Load column info + generate CREATE TABLE DDL for each table
    let mut table_infos: Vec<TableInfo> = Vec::new();
    for (schema, name) in &tables_filtered {
        let fqn = format!("[{schema}].[{name}]");
        let columns = query_columns(&mut conn, &fqn).await?;
        let create_ddl = generate_create_ddl(&schema, &name, &columns);
        let identity_columns = columns
            .iter()
            .filter(|c| c.is_identity)
            .map(|c| c.name.clone())
            .collect();

        table_infos.push(TableInfo {
            schema: schema.clone(),
            name: name.clone(),
            fqn,
            columns,
            identity_columns,
            create_ddl,
        });
    }

    // 4. Load FK info
    let fks = query_fks(&mut conn).await?;

    // 5. Topological sort of tables by FK dependencies
    let sorted_tables = topo_sort(table_infos, &fks);

    // 6. Load views, procs, functions DDL
    let extra_ddl = query_objects_ddl(&mut conn).await?;

    // 7. Assemble schema.sql
    let schema_sql = build_schema_sql(&sorted_tables, &fks, &extra_ddl);

    Ok(SchemaBundle {
        tables: sorted_tables,
        fks,
        schema_sql,
    })
}

// ── Queries ───────────────────────────────────────────────────────────────────

async fn query_tables(
    conn: &mut crate::connection::TiberiusClient,
) -> Result<Vec<(String, String)>> {
    let rows = conn
        .query(
            "SELECT s.name AS schema_name, t.name AS table_name \
             FROM sys.tables t \
             JOIN sys.schemas s ON s.schema_id = t.schema_id \
             WHERE t.is_ms_shipped = 0 \
               AND t.temporal_type != 1  -- exclude temporal history tables (shadow tables) \
             ORDER BY s.name, t.name",
            &[],
        )
        .await?
        .into_results()
        .await?;

    let mut result = Vec::new();
    for row in rows.into_iter().flatten() {
        let schema: &str = row.get(0).unwrap_or("");
        let name: &str = row.get(1).unwrap_or("");
        result.push((schema.to_string(), name.to_string()));
    }
    Ok(result)
}

async fn query_columns(
    conn: &mut crate::connection::TiberiusClient,
    table_fqn: &str,
) -> Result<Vec<ColumnInfo>> {
    let sql = format!(
        "SELECT c.name, tp.name AS type_name, c.max_length, c.precision, c.scale, \
                c.is_nullable, c.is_identity, \
                ic.seed_value, ic.increment_value \
         FROM sys.columns c \
         JOIN sys.types tp ON tp.user_type_id = c.user_type_id \
         LEFT JOIN sys.identity_columns ic \
             ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
         WHERE c.object_id = OBJECT_ID('{table_fqn}') \
           AND c.is_computed = 0   -- skip computed columns (not selectable via SELECT *) \
           AND c.is_hidden = 0     -- skip hidden temporal period columns \
         ORDER BY c.column_id"
    );

    let rows = conn.query(&sql, &[]).await?.into_results().await?;

    let mut columns = Vec::new();
    for row in rows.into_iter().flatten() {
        let name: &str = row.get(0).unwrap_or("");
        let sql_type: &str = row.get(1).unwrap_or("varchar");
        let max_length: i16 = row.get(2).unwrap_or(0);
        let precision: u8 = row.get(3).unwrap_or(0);
        let scale: u8 = row.get(4).unwrap_or(0);
        let is_nullable: bool = row.get(5).unwrap_or(true);
        let is_identity: bool = row.get(6).unwrap_or(false);
        let identity_seed: Option<i64> = row.get(7);
        let identity_increment: Option<i64> = row.get(8);

        columns.push(ColumnInfo {
            name: name.to_string(),
            sql_type: sql_type.to_string(),
            precision,
            scale,
            max_length,
            is_nullable,
            is_identity,
            identity_seed,
            identity_increment,
        });
    }
    Ok(columns)
}

async fn query_fks(
    conn: &mut crate::connection::TiberiusClient,
) -> Result<Vec<FkInfo>> {
    // Get FK names + parent/ref table info
    let rows = conn
        .query(
            "SELECT fk.name, \
                    SCHEMA_NAME(tp.schema_id) AS parent_schema, tp.name AS parent_table, \
                    SCHEMA_NAME(tr.schema_id) AS ref_schema, tr.name AS ref_table, \
                    fk.delete_referential_action_desc, fk.update_referential_action_desc \
             FROM sys.foreign_keys fk \
             JOIN sys.tables tp ON tp.object_id = fk.parent_object_id \
             JOIN sys.tables tr ON tr.object_id = fk.referenced_object_id \
             ORDER BY parent_schema, parent_table",
            &[],
        )
        .await?
        .into_results()
        .await?;

    let mut fks = Vec::new();
    for row in rows.into_iter().flatten() {
        let fk_name: &str = row.get(0).unwrap_or("");
        let parent_schema: &str = row.get(1).unwrap_or("dbo");
        let parent_table: &str = row.get(2).unwrap_or("");
        let ref_schema: &str = row.get(3).unwrap_or("dbo");
        let ref_table: &str = row.get(4).unwrap_or("");
        let on_delete: &str = row.get(5).unwrap_or("NO_ACTION");
        let on_update: &str = row.get(6).unwrap_or("NO_ACTION");

        let parent_fqn = format!("[{parent_schema}].[{parent_table}]");
        let ref_fqn = format!("[{ref_schema}].[{ref_table}]");

        // Get columns for this FK
        let col_rows = conn
            .query(
                &format!(
                    "SELECT cpa.name AS parent_col, cra.name AS ref_col \
                     FROM sys.foreign_key_columns fkc \
                     JOIN sys.foreign_keys fk ON fk.object_id = fkc.constraint_object_id \
                     JOIN sys.columns cpa ON cpa.object_id = fk.parent_object_id \
                         AND cpa.column_id = fkc.parent_column_id \
                     JOIN sys.columns cra ON cra.object_id = fk.referenced_object_id \
                         AND cra.column_id = fkc.referenced_column_id \
                     WHERE fk.name = '{fk_name}' \
                     ORDER BY fkc.constraint_column_id"
                ),
                &[],
            )
            .await?
            .into_results()
            .await?;

        let mut parent_cols: Vec<String> = Vec::new();
        let mut ref_cols: Vec<String> = Vec::new();
        for col_row in col_rows.into_iter().flatten() {
            let pc: &str = col_row.get(0).unwrap_or("");
            let rc: &str = col_row.get(1).unwrap_or("");
            parent_cols.push(format!("[{pc}]"));
            ref_cols.push(format!("[{rc}]"));
        }

        let on_delete_clause = referential_action_clause("ON DELETE", on_delete);
        let on_update_clause = referential_action_clause("ON UPDATE", on_update);

        let alter_stmt = format!(
            "ALTER TABLE {parent_fqn} ADD CONSTRAINT [{fk_name}] \
             FOREIGN KEY ({}) REFERENCES {ref_fqn} ({}){}{}",
            parent_cols.join(", "),
            ref_cols.join(", "),
            on_delete_clause,
            on_update_clause,
        );

        let drop_stmt = format!("ALTER TABLE {parent_fqn} DROP CONSTRAINT IF EXISTS [{fk_name}]");

        fks.push(FkInfo {
            name: fk_name.to_string(),
            parent_fqn,
            ref_fqn,
            alter_stmt,
            drop_stmt,
        });
    }
    Ok(fks)
}

async fn query_objects_ddl(
    conn: &mut crate::connection::TiberiusClient,
) -> Result<String> {
    let rows = conn
        .query(
            "SELECT o.type_desc, o.name, sm.definition \
             FROM sys.objects o \
             JOIN sys.sql_modules sm ON sm.object_id = o.object_id \
             WHERE o.is_ms_shipped = 0 \
               AND o.type IN ('V','P','FN','IF','TF') \
             ORDER BY o.type, o.name",
            &[],
        )
        .await?
        .into_results()
        .await?;

    let mut parts: Vec<String> = Vec::new();
    for row in rows.into_iter().flatten() {
        let definition: &str = row.get(2).unwrap_or("");
        if !definition.trim().is_empty() {
            parts.push(definition.to_string());
            parts.push("GO".to_string());
        }
    }
    Ok(parts.join("\n"))
}

// ── DDL generation ────────────────────────────────────────────────────────────

fn generate_create_ddl(schema: &str, table: &str, columns: &[ColumnInfo]) -> String {
    let mut lines: Vec<String> = Vec::new();
    let fqn = format!("[{schema}].[{table}]");

    lines.push(format!("CREATE TABLE {fqn} ("));

    let col_defs: Vec<String> = columns
        .iter()
        .map(|c| {
            let type_spec = column_type_spec(c);
            let nullability = if c.is_nullable { "NULL" } else { "NOT NULL" };
            let identity = if c.is_identity {
                let seed = c.identity_seed.unwrap_or(1);
                let incr = c.identity_increment.unwrap_or(1);
                format!(" IDENTITY({seed},{incr})")
            } else {
                String::new()
            };
            format!("    [{name}] {type_spec}{identity} {nullability}", name = c.name)
        })
        .collect();

    lines.push(col_defs.join(",\n"));
    lines.push(")".to_string());
    lines.join("\n")
}

fn column_type_spec(c: &ColumnInfo) -> String {
    match c.sql_type.to_lowercase().as_str() {
        "decimal" | "numeric" => format!("{}({},{})", c.sql_type, c.precision, c.scale),
        "varchar" | "nvarchar" | "char" | "nchar" => {
            let len = if c.max_length == -1 {
                "MAX".to_string()
            } else {
                let char_len = if c.sql_type.to_lowercase().starts_with('n') {
                    c.max_length / 2
                } else {
                    c.max_length
                };
                char_len.to_string()
            };
            format!("{}({})", c.sql_type, len)
        }
        "varbinary" | "binary" => {
            let len = if c.max_length == -1 { "MAX".to_string() } else { c.max_length.to_string() };
            format!("{}({})", c.sql_type, len)
        }
        "datetime2" | "time" | "datetimeoffset" => {
            // scale stores fractional seconds precision (0-7)
            if c.scale > 0 { format!("{}({})", c.sql_type, c.scale) } else { c.sql_type.clone() }
        }
        _ => c.sql_type.clone(),
    }
}

fn referential_action_clause(prefix: &str, action: &str) -> String {
    match action.to_uppercase().as_str() {
        "CASCADE" => format!(" {prefix} CASCADE"),
        "SET_NULL" | "SET NULL" => format!(" {prefix} SET NULL"),
        "SET_DEFAULT" | "SET DEFAULT" => format!(" {prefix} SET DEFAULT"),
        _ => String::new(), // NO_ACTION is the default, no clause needed
    }
}

// ── Topological sort (Kahn's algorithm) ──────────────────────────────────────

fn topo_sort(tables: Vec<TableInfo>, fks: &[FkInfo]) -> Vec<TableInfo> {
    // Build adjacency: parent_fqn → set of child fqns that depend on it
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

    for t in &tables {
        in_degree.entry(t.fqn.clone()).or_insert(0);
    }

    for fk in fks {
        // child (parent_fqn) depends on ref_fqn — ref must come first
        if fk.parent_fqn != fk.ref_fqn {
            *in_degree.entry(fk.parent_fqn.clone()).or_insert(0) += 1;
            dependents
                .entry(fk.ref_fqn.clone())
                .or_default()
                .push(fk.parent_fqn.clone());
        }
    }

    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(fqn, _)| fqn.clone())
        .collect();

    let table_map: HashMap<String, TableInfo> =
        tables.into_iter().map(|t| (t.fqn.clone(), t)).collect();

    let mut sorted: Vec<TableInfo> = Vec::new();

    while let Some(fqn) = queue.pop_front() {
        if let Some(t) = table_map.get(&fqn) {
            sorted.push(t.clone());
        }
        if let Some(children) = dependents.get(&fqn) {
            for child in children {
                let deg = in_degree.get_mut(child).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(child.clone());
                }
            }
        }
    }

    // Append any tables not in the sort (e.g. self-referential) at the end
    let sorted_fqns: HashSet<String> = sorted.iter().map(|t| t.fqn.clone()).collect();
    for (fqn, t) in &table_map {
        if !sorted_fqns.contains(fqn) {
            sorted.push(t.clone());
        }
    }

    sorted
}

// ── schema.sql assembly ───────────────────────────────────────────────────────

pub const POST_DATA_MARKER: &str = "-- SQLRUSTLER_SECTION: POST_DATA";

fn build_schema_sql(tables: &[TableInfo], fks: &[FkInfo], extra_ddl: &str) -> String {
    let ts = chrono::Utc::now().to_rfc3339();
    let mut out = Vec::<String>::new();

    out.push(format!("-- sqlrustler schema export"));
    out.push(format!("-- Generated: {ts}"));
    out.push(String::new());

    // DROP FOREIGN KEYS in reverse order
    out.push("-- === DROP FOREIGN KEYS (reverse topo order) ===".to_string());
    for fk in fks.iter().rev() {
        out.push(format!("{};", fk.drop_stmt));
    }
    out.push(String::new());

    // CREATE TABLES in topo order
    out.push("-- === CREATE TABLES (topo order) ===".to_string());
    for t in tables {
        out.push(format!("{};", t.create_ddl));
        out.push("GO".to_string());
    }
    out.push(String::new());

    // VIEWS / PROCS / FUNCTIONS
    if !extra_ddl.trim().is_empty() {
        out.push("-- === VIEWS / STORED PROCS / FUNCTIONS ===".to_string());
        out.push(extra_ddl.to_string());
        out.push(String::new());
    }

    // POST_DATA section — FK add statements (applied after data load on import)
    out.push(POST_DATA_MARKER.to_string());
    out.push(String::new());
    out.push("-- === ADD FOREIGN KEYS (forward topo order) ===".to_string());
    for fk in fks {
        out.push(format!("{};", fk.alter_stmt));
    }

    out.join("\n")
}

// ── Exclusion filter ──────────────────────────────────────────────────────────

fn is_excluded(schema: &str, table: &str, globs: &[String]) -> bool {
    if globs.is_empty() {
        return false;
    }
    let full = format!("{schema}.{table}");
    let short = table;
    for pattern in globs {
        if let Ok(g) = glob::Pattern::new(pattern) {
            if g.matches(short) || g.matches(&full) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn make_col(
        name: &str,
        sql_type: &str,
        max_length: i16,
        precision: u8,
        scale: u8,
        is_nullable: bool,
        is_identity: bool,
        identity_seed: Option<i64>,
        identity_increment: Option<i64>,
    ) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            sql_type: sql_type.to_string(),
            precision,
            scale,
            max_length,
            is_nullable,
            is_identity,
            identity_seed,
            identity_increment,
        }
    }

    fn make_table(schema: &str, name: &str, fqn: &str) -> TableInfo {
        TableInfo {
            schema: schema.to_string(),
            name: name.to_string(),
            fqn: fqn.to_string(),
            columns: vec![],
            identity_columns: vec![],
            create_ddl: String::new(),
        }
    }

    fn make_fk(parent_fqn: &str, ref_fqn: &str) -> FkInfo {
        FkInfo {
            name: format!("fk_{}_{}", parent_fqn, ref_fqn),
            parent_fqn: parent_fqn.to_string(),
            ref_fqn: ref_fqn.to_string(),
            alter_stmt: String::new(),
            drop_stmt: String::new(),
        }
    }

    // ── generate_create_ddl ───────────────────────────────────────────────────

    #[test]
    fn varchar_max_generates_varchar_max() {
        let col = make_col("Description", "varchar", -1, 0, 0, true, false, None, None);
        let ddl = generate_create_ddl("dbo", "Items", &[col]);
        assert!(ddl.contains("varchar(MAX)"), "DDL: {ddl}");
    }

    #[test]
    fn nvarchar_100_generates_nvarchar_100_from_max_length_200() {
        // SQL Server stores nvarchar max_length in bytes; 100 chars = 200 bytes
        let col = make_col("Title", "nvarchar", 200, 0, 0, true, false, None, None);
        let ddl = generate_create_ddl("dbo", "Items", &[col]);
        assert!(ddl.contains("nvarchar(100)"), "DDL: {ddl}");
    }

    #[test]
    fn decimal_18_4_in_ddl() {
        let col = make_col("Amount", "decimal", -1, 18, 4, true, false, None, None);
        let ddl = generate_create_ddl("dbo", "Orders", &[col]);
        assert!(ddl.contains("decimal(18,4)"), "DDL: {ddl}");
    }

    #[test]
    fn identity_column_includes_identity_clause() {
        let col = make_col("Id", "int", 4, 10, 0, false, true, Some(1), Some(1));
        let ddl = generate_create_ddl("dbo", "Orders", &[col]);
        assert!(ddl.contains("IDENTITY(1,1)"), "DDL: {ddl}");
    }

    #[test]
    fn not_null_column_includes_not_null() {
        let col = make_col("Code", "varchar", 50, 0, 0, false, false, None, None);
        let ddl = generate_create_ddl("dbo", "Items", &[col]);
        assert!(ddl.contains("NOT NULL"), "DDL: {ddl}");
    }

    #[test]
    fn nullable_column_includes_null() {
        let col = make_col("Note", "varchar", 500, 0, 0, true, false, None, None);
        let ddl = generate_create_ddl("dbo", "Items", &[col]);
        // Should contain NULL but not NOT NULL
        let stripped = ddl.replace("NOT NULL", "");
        assert!(stripped.contains("NULL"), "DDL: {ddl}");
        assert!(!ddl.contains("NOT NULL"), "DDL: {ddl}");
    }

    // ── column_type_spec ──────────────────────────────────────────────────────

    #[test]
    fn col_type_decimal_10_2() {
        let col = make_col("X", "decimal", -1, 10, 2, true, false, None, None);
        assert_eq!(column_type_spec(&col), "decimal(10,2)");
    }

    #[test]
    fn col_type_varchar_100() {
        let col = make_col("X", "varchar", 100, 0, 0, true, false, None, None);
        assert_eq!(column_type_spec(&col), "varchar(100)");
    }

    #[test]
    fn col_type_varchar_max() {
        let col = make_col("X", "varchar", -1, 0, 0, true, false, None, None);
        assert_eq!(column_type_spec(&col), "varchar(MAX)");
    }

    #[test]
    fn col_type_nvarchar_100_from_200_bytes() {
        let col = make_col("X", "nvarchar", 200, 0, 0, true, false, None, None);
        assert_eq!(column_type_spec(&col), "nvarchar(100)");
    }

    #[test]
    fn col_type_nvarchar_max() {
        let col = make_col("X", "nvarchar", -1, 0, 0, true, false, None, None);
        assert_eq!(column_type_spec(&col), "nvarchar(MAX)");
    }

    #[test]
    fn col_type_datetime2_scale7() {
        let col = make_col("X", "datetime2", 8, 0, 7, true, false, None, None);
        assert_eq!(column_type_spec(&col), "datetime2(7)");
    }

    #[test]
    fn col_type_datetime2_scale0_no_suffix() {
        let col = make_col("X", "datetime2", 6, 0, 0, true, false, None, None);
        assert_eq!(column_type_spec(&col), "datetime2");
    }

    #[test]
    fn col_type_int_passes_through() {
        let col = make_col("X", "int", 4, 10, 0, true, false, None, None);
        assert_eq!(column_type_spec(&col), "int");
    }

    // ── topo_sort ─────────────────────────────────────────────────────────────

    fn fqns(sorted: &[TableInfo]) -> Vec<&str> {
        sorted.iter().map(|t| t.fqn.as_str()).collect()
    }

    #[test]
    fn topo_sort_no_fks_all_tables_present() {
        let tables = vec![
            make_table("dbo", "A", "[dbo].[A]"),
            make_table("dbo", "B", "[dbo].[B]"),
            make_table("dbo", "C", "[dbo].[C]"),
        ];
        let sorted = topo_sort(tables, &[]);
        assert_eq!(sorted.len(), 3);
        let names = fqns(&sorted);
        assert!(names.contains(&"[dbo].[A]"));
        assert!(names.contains(&"[dbo].[B]"));
        assert!(names.contains(&"[dbo].[C]"));
    }

    #[test]
    fn topo_sort_a_depends_on_b_b_comes_first() {
        let tables = vec![
            make_table("dbo", "A", "[dbo].[A]"),
            make_table("dbo", "B", "[dbo].[B]"),
        ];
        // A → B means A has FK referencing B → B must come before A
        let fks = vec![make_fk("[dbo].[A]", "[dbo].[B]")];
        let sorted = topo_sort(tables, &fks);
        let names = fqns(&sorted);
        let pos_a = names.iter().position(|&x| x == "[dbo].[A]").unwrap();
        let pos_b = names.iter().position(|&x| x == "[dbo].[B]").unwrap();
        assert!(pos_b < pos_a, "B must precede A; got order: {names:?}");
    }

    #[test]
    fn topo_sort_three_table_chain() {
        // A → B → C means C first, then B, then A
        let tables = vec![
            make_table("dbo", "A", "[dbo].[A]"),
            make_table("dbo", "B", "[dbo].[B]"),
            make_table("dbo", "C", "[dbo].[C]"),
        ];
        let fks = vec![
            make_fk("[dbo].[A]", "[dbo].[B]"),
            make_fk("[dbo].[B]", "[dbo].[C]"),
        ];
        let sorted = topo_sort(tables, &fks);
        let names = fqns(&sorted);
        let pos_a = names.iter().position(|&x| x == "[dbo].[A]").unwrap();
        let pos_b = names.iter().position(|&x| x == "[dbo].[B]").unwrap();
        let pos_c = names.iter().position(|&x| x == "[dbo].[C]").unwrap();
        assert!(pos_c < pos_b, "C before B; order: {names:?}");
        assert!(pos_b < pos_a, "B before A; order: {names:?}");
    }

    #[test]
    fn topo_sort_diamond_d_comes_last_a_comes_first() {
        // A→B, A→C, B→D, C→D  → D must be before B and C, A must be after B and C
        let tables = vec![
            make_table("dbo", "A", "[dbo].[A]"),
            make_table("dbo", "B", "[dbo].[B]"),
            make_table("dbo", "C", "[dbo].[C]"),
            make_table("dbo", "D", "[dbo].[D]"),
        ];
        let fks = vec![
            make_fk("[dbo].[A]", "[dbo].[B]"),
            make_fk("[dbo].[A]", "[dbo].[C]"),
            make_fk("[dbo].[B]", "[dbo].[D]"),
            make_fk("[dbo].[C]", "[dbo].[D]"),
        ];
        let sorted = topo_sort(tables, &fks);
        assert_eq!(sorted.len(), 4);
        let names = fqns(&sorted);
        let pos_a = names.iter().position(|&x| x == "[dbo].[A]").unwrap();
        let pos_b = names.iter().position(|&x| x == "[dbo].[B]").unwrap();
        let pos_c = names.iter().position(|&x| x == "[dbo].[C]").unwrap();
        let pos_d = names.iter().position(|&x| x == "[dbo].[D]").unwrap();
        // D must come before B and C; A must come after B and C
        assert!(pos_d < pos_b, "D before B; order: {names:?}");
        assert!(pos_d < pos_c, "D before C; order: {names:?}");
        assert!(pos_b < pos_a, "B before A; order: {names:?}");
        assert!(pos_c < pos_a, "C before A; order: {names:?}");
    }

    #[test]
    fn topo_sort_self_referential_fk_appears_exactly_once() {
        // A → A (self-ref) — self-referential FK; table must appear exactly once
        let tables = vec![make_table("dbo", "A", "[dbo].[A]")];
        let fks = vec![make_fk("[dbo].[A]", "[dbo].[A]")];
        let sorted = topo_sort(tables, &fks);
        let count = sorted.iter().filter(|t| t.fqn == "[dbo].[A]").count();
        assert_eq!(count, 1, "self-ref table must appear exactly once");
    }

    // ── is_excluded ───────────────────────────────────────────────────────────

    #[test]
    fn glob_tmp_star_matches_tmp_staging() {
        assert!(is_excluded("dbo", "tmp_staging", &["tmp_*".to_string()]));
    }

    #[test]
    fn glob_tmp_star_does_not_match_atmp() {
        assert!(!is_excluded("dbo", "atmp", &["tmp_*".to_string()]));
    }

    #[test]
    fn glob_log_star_matches_log_errors() {
        assert!(is_excluded("dbo", "log_errors", &["log_*".to_string()]));
    }

    #[test]
    fn empty_globs_never_excluded() {
        assert!(!is_excluded("dbo", "tmp_staging", &[]));
        assert!(!is_excluded("dbo", "anything", &[]));
    }

    #[test]
    fn schema_qualified_glob_matches_full_name() {
        // pattern "archive.*" matches schema-qualified "archive.OldOrders"
        assert!(is_excluded("archive", "OldOrders", &["archive.*".to_string()]));
    }
}
