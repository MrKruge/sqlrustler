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

/// DDL-only column metadata that the data path (export/Parquet) does not need.
/// Kept out of `ColumnInfo` so that struct stays stable for external users.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ColumnExtra {
    /// Column collation (char/varchar/nchar/nvarchar/text/ntext). Without it a
    /// non-default-collation varchar column is recreated with the target DB's default
    /// collation and non-ASCII data is silently replaced by '?' on import.
    pub collation: Option<String>,
    /// (constraint name, definition) of the DEFAULT constraint, if any.
    pub default: Option<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    /// Fully-qualified bracketed name, e.g. "[dbo].[Orders]"
    pub fqn: String,
    pub columns: Vec<ColumnInfo>,
    pub identity_columns: Vec<String>,
    /// Best integer column to use for PK-range chunking.
    /// Either the single IDENTITY column, or the first key column of the clustered
    /// integer PK (for tables imported without IDENTITY, e.g. StackExchange data dumps).
    pub chunk_key: Option<String>,
    /// True only when chunk_key is backed by an actual index (safe to chunk).
    /// False = fallback column name only — chunking would cause parallel full scans.
    pub chunk_key_indexed: bool,
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

/// PRIMARY KEY / UNIQUE / CHECK constraint. Emitted in the POST_DATA section, before the
/// FKs (a FK needs a PK/UNIQUE key on the referenced columns to be creatable).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConstraintInfo {
    pub table_fqn: String,
    pub name: String,
    /// Full ALTER TABLE ADD CONSTRAINT statement
    pub add_stmt: String,
}

#[allow(dead_code)]
pub struct SchemaBundle {
    /// Tables in topological order (parents before children)
    pub tables: Vec<TableInfo>,
    pub fks: Vec<FkInfo>,
    pub constraints: Vec<ConstraintInfo>,
    /// Complete schema.sql content with POST_DATA marker
    pub schema_sql: String,
}

// ── Identifier / literal quoting ──────────────────────────────────────────────

/// Bracket-quote a SQL Server identifier. `]` is escaped by doubling it.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// `[schema].[name]` with both parts safely quoted.
pub(crate) fn qualified_name(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}

/// Escape a value for use inside a single-quoted T-SQL string literal.
pub(crate) fn sql_literal_body(value: &str) -> String {
    value.replace('\'', "''")
}

// ── Main extraction entry point ───────────────────────────────────────────────

pub async fn extract(pool: &DbPool, exclude_globs: &[String]) -> Result<SchemaBundle> {
    let mut conn = pool
        .get()
        .await
        .context("Failed to get connection for schema extraction")?;

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
        let fqn = qualified_name(schema, name);
        let (columns, extras) = query_columns(&mut conn, &fqn).await?;
        let computed = query_computed_columns(&mut conn, &fqn).await?;
        let create_ddl = generate_create_ddl_full(schema, name, &columns, &extras, &computed);
        let identity_columns: Vec<String> = columns
            .iter()
            .filter(|c| c.is_identity)
            .map(|c| c.name.clone())
            .collect();

        // Determine the best column for PK-range chunking:
        // 1. Single IDENTITY column (preferred — guaranteed unique, monotonic)
        // 2. First key column of a single-column indexed integer PK
        // 3. Fallback: unindexed column named 'Id' (NOT safe to chunk — parallel full scans)
        let (chunk_key, chunk_key_indexed) = if identity_columns.len() == 1 {
            // IDENTITY columns always have a clustered index by convention
            (Some(identity_columns[0].clone()), true)
        } else {
            let indexed = query_clustered_int_pk(&mut conn, &fqn).await?;
            if indexed.is_some() {
                (indexed, true)
            } else {
                let fallback = query_fallback_int_col(&mut conn, &fqn).await?;
                (fallback, false)
            }
        };

        table_infos.push(TableInfo {
            schema: schema.clone(),
            name: name.clone(),
            fqn,
            columns,
            identity_columns,
            chunk_key,
            chunk_key_indexed,
            create_ddl,
        });
    }

    // 4. Load FK + key/check constraints. Anything touching a table we are not exporting
    //    (excluded via globs) is dropped — those statements would fail on import.
    let fks = retain_fks_between(query_fks(&mut conn).await?, &table_infos);
    let mut constraints = query_keys(&mut conn).await?;
    constraints.extend(query_checks(&mut conn).await?);
    let constraints = retain_constraints_for(constraints, &table_infos);

    // 5. Topological sort of tables by FK dependencies
    let sorted_tables = topo_sort(table_infos, &fks);

    // 6. Load schemas + views, procs, functions DDL
    let schemas = query_schemas(&mut conn).await?;
    let extra_ddl = query_objects_ddl(&mut conn).await?;

    // 7. Assemble schema.sql
    let schema_sql =
        build_schema_sql_full(&schemas, &sorted_tables, &fks, &constraints, &extra_ddl);

    Ok(SchemaBundle {
        tables: sorted_tables,
        fks,
        constraints,
        schema_sql,
    })
}

// ── SQL text builders (separate fns so they can be unit-tested) ───────────────
//
// NOTE: never put `--` line comments inside these strings. Rust `\` line continuations
// join the whole statement onto ONE line, so a `--` comment would silently comment out
// the rest of the query (this used to drop the key_ordinal / type filters / ORDER BY).

const TABLES_SQL: &str = "SELECT s.name AS schema_name, t.name AS table_name \
     FROM sys.tables t \
     JOIN sys.schemas s ON s.schema_id = t.schema_id \
     WHERE t.is_ms_shipped = 0 \
       AND t.temporal_type != 1 \
     ORDER BY s.name, t.name";

/// `OBJECT_ID(N'...')` expression for a bracketed table FQN, literal-escaped.
fn object_id_expr(table_fqn: &str) -> String {
    format!("OBJECT_ID(N'{}')", sql_literal_body(table_fqn))
}

fn columns_sql(table_fqn: &str) -> String {
    // Alias types (CREATE TYPE ... FROM nvarchar(50), sysname) are resolved to their base
    // system type; CLR types (hierarchyid/geography/geometry, is_assembly_type = 1) are kept.
    // Computed columns are skipped here (see computed_columns_sql); hidden temporal period
    // columns are skipped.
    format!(
        "SELECT c.name, COALESCE(bt.name, tp.name) AS type_name, c.max_length, c.precision, c.scale, \
                c.is_nullable, c.is_identity, \
                CAST(ic.seed_value AS BIGINT) AS seed_value, \
                CAST(ic.increment_value AS BIGINT) AS increment_value, \
                c.collation_name, dc.name AS default_name, dc.definition AS default_definition \
         FROM sys.columns c \
         JOIN sys.types tp ON tp.user_type_id = c.user_type_id \
         LEFT JOIN sys.types bt ON bt.user_type_id = tp.system_type_id \
             AND tp.is_assembly_type = 0 AND tp.user_type_id <> tp.system_type_id \
         LEFT JOIN sys.identity_columns ic \
             ON ic.object_id = c.object_id AND ic.column_id = c.column_id \
         LEFT JOIN sys.default_constraints dc \
             ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id \
         WHERE c.object_id = {} \
           AND c.is_computed = 0 \
           AND c.is_hidden = 0 \
         ORDER BY c.column_id",
        object_id_expr(table_fqn)
    )
}

fn computed_columns_sql(table_fqn: &str) -> String {
    format!(
        "SELECT c.name, cc.definition, cc.is_persisted \
         FROM sys.computed_columns cc \
         JOIN sys.columns c ON c.object_id = cc.object_id AND c.column_id = cc.column_id \
         WHERE cc.object_id = {} \
         ORDER BY c.column_id",
        object_id_expr(table_fqn)
    )
}

fn clustered_int_pk_sql(table_fqn: &str) -> String {
    format!(
        "SELECT TOP 1 c.name \
         FROM sys.indexes i \
         JOIN sys.index_columns ic \
             ON ic.object_id = i.object_id AND ic.index_id = i.index_id \
         JOIN sys.columns c \
             ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
         JOIN sys.types tp ON tp.user_type_id = c.user_type_id \
         WHERE i.object_id = {} \
           AND i.index_id > 0 \
           AND ic.key_ordinal = 1 \
           AND ic.is_descending_key = 0 \
           AND tp.name IN ('int', 'bigint') \
           AND NOT EXISTS ( \
               SELECT 1 FROM sys.index_columns ic2 \
               WHERE ic2.object_id = i.object_id \
                 AND ic2.index_id = i.index_id \
                 AND ic2.key_ordinal = 2 \
           ) \
         ORDER BY \
           CASE WHEN i.is_primary_key = 1 THEN 0 ELSE 1 END, \
           CASE WHEN i.is_unique = 1 THEN 0 ELSE 1 END, \
           CASE WHEN i.type = 1 THEN 0 ELSE 1 END, \
           i.index_id ASC",
        object_id_expr(table_fqn)
    )
}

fn fallback_int_col_sql(table_fqn: &str) -> String {
    format!(
        "SELECT TOP 1 c.name \
         FROM sys.columns c \
         JOIN sys.types tp ON tp.user_type_id = c.user_type_id \
         WHERE c.object_id = {} \
           AND c.is_nullable = 0 \
           AND tp.name IN ('int', 'bigint') \
           AND (c.is_identity = 1 OR LOWER(c.name) = 'id') \
         ORDER BY c.is_identity DESC, c.column_id ASC",
        object_id_expr(table_fqn)
    )
}

const FKS_SQL: &str = "SELECT fk.name, \
            SCHEMA_NAME(tp.schema_id) AS parent_schema, tp.name AS parent_table, \
            SCHEMA_NAME(tr.schema_id) AS ref_schema, tr.name AS ref_table, \
            fk.delete_referential_action_desc, fk.update_referential_action_desc, \
            fk.object_id, fk.is_disabled, fk.is_not_trusted \
     FROM sys.foreign_keys fk \
     JOIN sys.tables tp ON tp.object_id = fk.parent_object_id \
     JOIN sys.tables tr ON tr.object_id = fk.referenced_object_id \
     ORDER BY parent_schema, parent_table, fk.name";

/// FK column pairs, addressed by the constraint's object_id (FK names are only unique per
/// schema, so a name-based lookup would merge the columns of same-named FKs).
fn fk_columns_sql(fk_object_id: i32) -> String {
    format!(
        "SELECT cpa.name AS parent_col, cra.name AS ref_col \
         FROM sys.foreign_key_columns fkc \
         JOIN sys.foreign_keys fk ON fk.object_id = fkc.constraint_object_id \
         JOIN sys.columns cpa ON cpa.object_id = fk.parent_object_id \
             AND cpa.column_id = fkc.parent_column_id \
         JOIN sys.columns cra ON cra.object_id = fk.referenced_object_id \
             AND cra.column_id = fkc.referenced_column_id \
         WHERE fk.object_id = {fk_object_id} \
         ORDER BY fkc.constraint_column_id"
    )
}

const KEYS_SQL: &str = "SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, \
            kc.name, kc.type, i.type_desc, t.object_id, i.index_id \
     FROM sys.key_constraints kc \
     JOIN sys.tables t ON t.object_id = kc.parent_object_id \
     JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id \
     WHERE t.is_ms_shipped = 0 \
     ORDER BY schema_name, table_name, CASE WHEN kc.type = 'PK' THEN 0 ELSE 1 END, kc.name";

fn key_columns_sql(table_object_id: i32, index_id: i32) -> String {
    format!(
        "SELECT c.name, ic.is_descending_key \
         FROM sys.index_columns ic \
         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id \
         WHERE ic.object_id = {table_object_id} AND ic.index_id = {index_id} \
           AND ic.is_included_column = 0 AND ic.key_ordinal > 0 \
         ORDER BY ic.key_ordinal"
    )
}

const CHECKS_SQL: &str = "SELECT SCHEMA_NAME(t.schema_id) AS schema_name, t.name AS table_name, \
            cc.name, cc.definition, cc.is_disabled, cc.is_not_trusted \
     FROM sys.check_constraints cc \
     JOIN sys.tables t ON t.object_id = cc.parent_object_id \
     WHERE t.is_ms_shipped = 0 \
     ORDER BY schema_name, table_name, cc.name";

const SCHEMAS_SQL: &str = "SELECT DISTINCT SCHEMA_NAME(o.schema_id) AS schema_name \
     FROM sys.objects o \
     WHERE o.is_ms_shipped = 0 AND o.type IN ('U','V','P','FN','IF','TF')";

const OBJECTS_SQL: &str = "SELECT o.type_desc, o.name, sm.definition \
     FROM sys.objects o \
     JOIN sys.sql_modules sm ON sm.object_id = o.object_id \
     WHERE o.is_ms_shipped = 0 \
       AND o.type IN ('V','P','FN','IF','TF') \
     ORDER BY o.type, o.name";

// ── Queries ───────────────────────────────────────────────────────────────────

async fn query_tables(
    conn: &mut crate::connection::TiberiusClient,
) -> Result<Vec<(String, String)>> {
    let rows = conn.query(TABLES_SQL, &[]).await?.into_results().await?;

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
) -> Result<(Vec<ColumnInfo>, Vec<ColumnExtra>)> {
    let sql = columns_sql(table_fqn);
    let rows = conn.query(&sql, &[]).await?.into_results().await?;

    let mut columns = Vec::new();
    let mut extras = Vec::new();
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
        let collation: Option<&str> = row.get(9);
        let default_name: Option<&str> = row.get(10);
        let default_def: Option<&str> = row.get(11);

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
        extras.push(ColumnExtra {
            collation: collation.map(str::to_string),
            default: match (default_name, default_def) {
                (Some(n), Some(d)) => Some((n.to_string(), d.to_string())),
                _ => None,
            },
        });
    }
    Ok((columns, extras))
}

/// Computed columns are not exported as data (the server recomputes them) but must be part
/// of CREATE TABLE. Returns ready-to-use column definition lines.
async fn query_computed_columns(
    conn: &mut crate::connection::TiberiusClient,
    table_fqn: &str,
) -> Result<Vec<String>> {
    let sql = computed_columns_sql(table_fqn);
    let rows = conn.query(&sql, &[]).await?.into_results().await?;
    let mut out = Vec::new();
    for row in rows.into_iter().flatten() {
        let name: &str = row.get(0).unwrap_or("");
        let definition: &str = row.get(1).unwrap_or("");
        let persisted: bool = row.get(2).unwrap_or(false);
        out.push(computed_column_def(name, definition, persisted));
    }
    Ok(out)
}

/// Returns the best integer column to use for PK-range chunking.
/// Accepts: PK (clustered or non-clustered), unique clustered index, or any clustered index.
/// Single-column only. Prefers: PK > unique > clustered. Covers StackExchange imports
/// which use NONCLUSTERED PKs or bare clustered indexes without a formal PK constraint.
async fn query_clustered_int_pk(
    conn: &mut crate::connection::TiberiusClient,
    table_fqn: &str,
) -> Result<Option<String>> {
    let sql = clustered_int_pk_sql(table_fqn);
    let rows = conn.query(&sql, &[]).await?.into_results().await?;
    let col_name = rows
        .into_iter()
        .flatten()
        .next()
        .and_then(|r| r.get::<&str, _>(0).map(|s| s.to_string()));
    Ok(col_name)
}

/// Fallback for heap tables with no index: find a NOT NULL INT/BIGINT column named 'Id'
/// or an IDENTITY column. chunk_key_indexed will be false — do NOT chunk these tables.
async fn query_fallback_int_col(
    conn: &mut crate::connection::TiberiusClient,
    table_fqn: &str,
) -> Result<Option<String>> {
    let sql = fallback_int_col_sql(table_fqn);
    let rows = conn.query(&sql, &[]).await?.into_results().await?;
    Ok(rows
        .into_iter()
        .flatten()
        .next()
        .and_then(|r| r.get::<&str, _>(0).map(|s| s.to_string())))
}

async fn query_fks(conn: &mut crate::connection::TiberiusClient) -> Result<Vec<FkInfo>> {
    let rows = conn.query(FKS_SQL, &[]).await?.into_results().await?;

    let mut fks = Vec::new();
    for row in rows.into_iter().flatten() {
        let fk_name: &str = row.get(0).unwrap_or("");
        let parent_schema: &str = row.get(1).unwrap_or("dbo");
        let parent_table: &str = row.get(2).unwrap_or("");
        let ref_schema: &str = row.get(3).unwrap_or("dbo");
        let ref_table: &str = row.get(4).unwrap_or("");
        let on_delete: &str = row.get(5).unwrap_or("NO_ACTION");
        let on_update: &str = row.get(6).unwrap_or("NO_ACTION");
        let fk_object_id: i32 = row.get(7).unwrap_or(0);
        let is_disabled: bool = row.get(8).unwrap_or(false);
        let is_not_trusted: bool = row.get(9).unwrap_or(false);

        let col_rows = conn
            .query(&fk_columns_sql(fk_object_id), &[])
            .await?
            .into_results()
            .await?;

        let mut parent_cols: Vec<String> = Vec::new();
        let mut ref_cols: Vec<String> = Vec::new();
        for col_row in col_rows.into_iter().flatten() {
            let pc: &str = col_row.get(0).unwrap_or("");
            let rc: &str = col_row.get(1).unwrap_or("");
            parent_cols.push(quote_ident(pc));
            ref_cols.push(quote_ident(rc));
        }

        fks.push(build_fk(
            fk_name,
            (parent_schema, parent_table),
            (ref_schema, ref_table),
            &parent_cols,
            &ref_cols,
            on_delete,
            on_update,
            is_disabled,
            is_not_trusted,
        ));
    }
    Ok(fks)
}

async fn query_keys(conn: &mut crate::connection::TiberiusClient) -> Result<Vec<ConstraintInfo>> {
    let rows = conn.query(KEYS_SQL, &[]).await?.into_results().await?;
    let mut out = Vec::new();
    for row in rows.into_iter().flatten() {
        let schema: &str = row.get(0).unwrap_or("dbo");
        let table: &str = row.get(1).unwrap_or("");
        let name: &str = row.get(2).unwrap_or("");
        let kind: &str = row.get(3).unwrap_or("UQ");
        let type_desc: &str = row.get(4).unwrap_or("NONCLUSTERED");
        let object_id: i32 = row.get(5).unwrap_or(0);
        let index_id: i32 = row.get(6).unwrap_or(0);

        let col_rows = conn
            .query(&key_columns_sql(object_id, index_id), &[])
            .await?
            .into_results()
            .await?;
        let cols: Vec<(String, bool)> = col_rows
            .into_iter()
            .flatten()
            .map(|r| {
                (
                    r.get::<&str, _>(0).unwrap_or("").to_string(),
                    r.get::<bool, _>(1).unwrap_or(false),
                )
            })
            .collect();
        if cols.is_empty() {
            continue;
        }
        out.push(build_key_constraint(
            schema,
            table,
            name,
            kind.trim(),
            type_desc,
            &cols,
        ));
    }
    Ok(out)
}

async fn query_checks(conn: &mut crate::connection::TiberiusClient) -> Result<Vec<ConstraintInfo>> {
    let rows = conn.query(CHECKS_SQL, &[]).await?.into_results().await?;
    let mut out = Vec::new();
    for row in rows.into_iter().flatten() {
        let schema: &str = row.get(0).unwrap_or("dbo");
        let table: &str = row.get(1).unwrap_or("");
        let name: &str = row.get(2).unwrap_or("");
        let definition: &str = row.get(3).unwrap_or("");
        let is_disabled: bool = row.get(4).unwrap_or(false);
        let is_not_trusted: bool = row.get(5).unwrap_or(false);
        if definition.trim().is_empty() {
            continue;
        }
        out.push(build_check_constraint(
            schema,
            table,
            name,
            definition,
            is_disabled,
            is_not_trusted,
        ));
    }
    Ok(out)
}

async fn query_schemas(conn: &mut crate::connection::TiberiusClient) -> Result<Vec<String>> {
    let rows = conn.query(SCHEMAS_SQL, &[]).await?.into_results().await?;
    let mut out: Vec<String> = rows
        .into_iter()
        .flatten()
        .filter_map(|r| r.get::<&str, _>(0).map(|s| s.to_string()))
        .collect();
    out.sort();
    Ok(out)
}

async fn query_objects_ddl(conn: &mut crate::connection::TiberiusClient) -> Result<String> {
    let rows = conn.query(OBJECTS_SQL, &[]).await?.into_results().await?;

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

// ── Constraint statement builders ─────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_fk(
    fk_name: &str,
    parent: (&str, &str),
    referenced: (&str, &str),
    parent_cols: &[String],
    ref_cols: &[String],
    on_delete: &str,
    on_update: &str,
    is_disabled: bool,
    is_not_trusted: bool,
) -> FkInfo {
    let parent_fqn = qualified_name(parent.0, parent.1);
    let ref_fqn = qualified_name(referenced.0, referenced.1);
    let qname = quote_ident(fk_name);

    // Preserve the source trust/enabled state: re-validating a FK the source itself never
    // validated (NOCHECK) can fail the whole post-data phase on legacy data.
    let check = if is_disabled || is_not_trusted {
        "NOCHECK"
    } else {
        "CHECK"
    };
    let mut alter_stmt = format!(
        "ALTER TABLE {parent_fqn} WITH {check} ADD CONSTRAINT {qname} \
         FOREIGN KEY ({}) REFERENCES {ref_fqn} ({}){}{}",
        parent_cols.join(", "),
        ref_cols.join(", "),
        referential_action_clause("ON DELETE", on_delete),
        referential_action_clause("ON UPDATE", on_update),
    );
    if is_disabled {
        alter_stmt.push_str(&format!(
            ";\nALTER TABLE {parent_fqn} NOCHECK CONSTRAINT {qname}"
        ));
    }

    // Guarded: on a fresh database the table does not exist yet when the DROP section runs.
    // Plain IF ... ALTER (no EXEC) so the import-side DDL allowlist can validate it.
    let drop_stmt = format!(
        "IF OBJECT_ID(N'{}', N'U') IS NOT NULL ALTER TABLE {parent_fqn} DROP CONSTRAINT IF EXISTS {qname}",
        sql_literal_body(&parent_fqn),
    );

    FkInfo {
        name: fk_name.to_string(),
        parent_fqn,
        ref_fqn,
        alter_stmt,
        drop_stmt,
    }
}

fn build_key_constraint(
    schema: &str,
    table: &str,
    name: &str,
    kind: &str,
    type_desc: &str,
    cols: &[(String, bool)],
) -> ConstraintInfo {
    let fqn = qualified_name(schema, table);
    let keyword = if kind == "PK" {
        "PRIMARY KEY"
    } else {
        "UNIQUE"
    };
    let clustering = if type_desc.eq_ignore_ascii_case("CLUSTERED") {
        "CLUSTERED"
    } else {
        "NONCLUSTERED"
    };
    let col_list: Vec<String> = cols
        .iter()
        .map(|(c, desc)| format!("{}{}", quote_ident(c), if *desc { " DESC" } else { " ASC" }))
        .collect();
    ConstraintInfo {
        table_fqn: fqn.clone(),
        name: name.to_string(),
        add_stmt: format!(
            "ALTER TABLE {fqn} ADD CONSTRAINT {} {keyword} {clustering} ({})",
            quote_ident(name),
            col_list.join(", ")
        ),
    }
}

fn build_check_constraint(
    schema: &str,
    table: &str,
    name: &str,
    definition: &str,
    is_disabled: bool,
    is_not_trusted: bool,
) -> ConstraintInfo {
    let fqn = qualified_name(schema, table);
    let qname = quote_ident(name);
    let check = if is_disabled || is_not_trusted {
        "NOCHECK"
    } else {
        "CHECK"
    };
    let mut stmt =
        format!("ALTER TABLE {fqn} WITH {check} ADD CONSTRAINT {qname} CHECK {definition}");
    if is_disabled {
        stmt.push_str(&format!(";\nALTER TABLE {fqn} NOCHECK CONSTRAINT {qname}"));
    }
    ConstraintInfo {
        table_fqn: fqn,
        name: name.to_string(),
        add_stmt: stmt,
    }
}

fn retain_fks_between(fks: Vec<FkInfo>, tables: &[TableInfo]) -> Vec<FkInfo> {
    let known: HashSet<&str> = tables.iter().map(|t| t.fqn.as_str()).collect();
    fks.into_iter()
        .filter(|fk| {
            let keep =
                known.contains(fk.parent_fqn.as_str()) && known.contains(fk.ref_fqn.as_str());
            if !keep {
                tracing::warn!(
                    "Skipping FK {} ({} -> {}): table is excluded from the export",
                    fk.name,
                    fk.parent_fqn,
                    fk.ref_fqn
                );
            }
            keep
        })
        .collect()
}

fn retain_constraints_for(cs: Vec<ConstraintInfo>, tables: &[TableInfo]) -> Vec<ConstraintInfo> {
    let known: HashSet<&str> = tables.iter().map(|t| t.fqn.as_str()).collect();
    cs.into_iter()
        .filter(|c| known.contains(c.table_fqn.as_str()))
        .collect()
}

// ── DDL generation ────────────────────────────────────────────────────────────

pub(crate) fn computed_column_def(name: &str, definition: &str, persisted: bool) -> String {
    format!(
        "{} AS {}{}",
        quote_ident(name),
        definition,
        if persisted { " PERSISTED" } else { "" }
    )
}

#[cfg(test)]
fn generate_create_ddl(schema: &str, table: &str, columns: &[ColumnInfo]) -> String {
    generate_create_ddl_full(schema, table, columns, &[], &[])
}

fn generate_create_ddl_full(
    schema: &str,
    table: &str,
    columns: &[ColumnInfo],
    extras: &[ColumnExtra],
    computed: &[String],
) -> String {
    let mut lines: Vec<String> = Vec::new();
    let fqn = qualified_name(schema, table);

    lines.push(format!("CREATE TABLE {fqn} ("));

    let mut col_defs: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let extra = extras.get(i);
            let type_spec = column_type_spec(c);
            let collate = extra
                .and_then(|e| e.collation.as_deref())
                .filter(|n| is_safe_collation(n))
                .map(|n| format!(" COLLATE {n}"))
                .unwrap_or_default();
            let nullability = if c.is_nullable { "NULL" } else { "NOT NULL" };
            let identity = if c.is_identity {
                let seed = c.identity_seed.unwrap_or(1);
                let incr = c.identity_increment.unwrap_or(1);
                format!(" IDENTITY({seed},{incr})")
            } else {
                String::new()
            };
            let default = extra
                .and_then(|e| e.default.as_ref())
                .map(|(n, d)| format!(" CONSTRAINT {} DEFAULT {d}", quote_ident(n)))
                .unwrap_or_default();
            format!(
                "    {name} {type_spec}{collate}{identity} {nullability}{default}",
                name = quote_ident(&c.name)
            )
        })
        .collect();
    col_defs.extend(computed.iter().map(|d| format!("    {d}")));

    lines.push(col_defs.join(",\n"));
    lines.push(")".to_string());
    lines.join("\n")
}

/// Collation names are plain identifiers (e.g. SQL_Latin1_General_CP1_CI_AS). Anything else
/// is not interpolated into DDL.
fn is_safe_collation(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
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
            let len = if c.max_length == -1 {
                "MAX".to_string()
            } else {
                c.max_length.to_string()
            };
            format!("{}({})", c.sql_type, len)
        }
        "datetime2" | "time" | "datetimeoffset" => {
            // scale = fractional-seconds precision (0-7). Always emitted: a bare
            // `time`/`datetime2`/`datetimeoffset` means scale 7, so omitting "(0)" would
            // silently widen a time(0) column to time(7).
            format!("{}({})", c.sql_type, c.scale)
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
    // Input order is preserved wherever the dependency graph allows, so output is
    // deterministic (the previous HashMap iteration made schema.sql differ run to run).
    let order: Vec<String> = tables.iter().map(|t| t.fqn.clone()).collect();
    let known: HashSet<&str> = order.iter().map(String::as_str).collect();

    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

    for fqn in &order {
        in_degree.entry(fqn.clone()).or_insert(0);
    }

    for fk in fks {
        // Edges to/from tables that are not part of this export are ignored; counting them
        // would leave a table with in_degree > 0 forever (mis-detected as a cycle).
        if fk.parent_fqn != fk.ref_fqn
            && known.contains(fk.parent_fqn.as_str())
            && known.contains(fk.ref_fqn.as_str())
        {
            *in_degree.entry(fk.parent_fqn.clone()).or_insert(0) += 1;
            dependents
                .entry(fk.ref_fqn.clone())
                .or_default()
                .push(fk.parent_fqn.clone());
        }
    }

    let mut queue: VecDeque<String> = order
        .iter()
        .filter(|fqn| in_degree.get(*fqn).copied() == Some(0))
        .cloned()
        .collect();

    let mut table_map: HashMap<String, TableInfo> =
        tables.into_iter().map(|t| (t.fqn.clone(), t)).collect();

    let mut sorted: Vec<TableInfo> = Vec::new();

    while let Some(fqn) = queue.pop_front() {
        if let Some(t) = table_map.remove(&fqn) {
            sorted.push(t);
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

    // Anything left is part of (or depends on) a genuine FK cycle. All FKs are applied in
    // POST_DATA so the order is not a correctness problem, but say so.
    if !table_map.is_empty() {
        let mut leftover: Vec<String> = order
            .iter()
            .filter(|f| table_map.contains_key(*f))
            .cloned()
            .collect();
        tracing::warn!(
            "{} table(s) are part of, or depend on, a circular foreign-key chain; appended in name order: {}",
            leftover.len(),
            leftover.join(", ")
        );
        for fqn in leftover.drain(..) {
            if let Some(t) = table_map.remove(&fqn) {
                sorted.push(t);
            }
        }
    }

    sorted
}

// ── schema.sql assembly ───────────────────────────────────────────────────────

pub const POST_DATA_MARKER: &str = "-- SQLRUSTLER_SECTION: POST_DATA";

fn build_schema_sql_full(
    schemas: &[String],
    tables: &[TableInfo],
    fks: &[FkInfo],
    constraints: &[ConstraintInfo],
    extra_ddl: &str,
) -> String {
    let ts = chrono::Utc::now().to_rfc3339();
    let mut out = Vec::<String>::new();

    out.push("-- sqlrustler schema export".to_string());
    out.push(format!("-- Generated: {ts}"));
    out.push(String::new());

    // CREATE SCHEMA for every non-dbo schema that owns an exported object. Without this
    // CREATE TABLE [sales].[Orders] fails on a fresh database.
    let create_schemas: Vec<&String> = schemas
        .iter()
        .filter(|s| !s.eq_ignore_ascii_case("dbo") && !s.is_empty())
        .collect();
    if !create_schemas.is_empty() {
        out.push("-- === CREATE SCHEMAS ===".to_string());
        for s in create_schemas {
            // CREATE SCHEMA must be alone in its batch.
            out.push(format!("CREATE SCHEMA {};", quote_ident(s)));
            out.push("GO".to_string());
        }
        out.push(String::new());
    }

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

    // POST_DATA section — keys, checks, FKs (applied after data load on import)
    out.push(POST_DATA_MARKER.to_string());
    out.push(String::new());
    if !constraints.is_empty() {
        out.push("-- === ADD PRIMARY KEY / UNIQUE / CHECK CONSTRAINTS ===".to_string());
        for c in constraints {
            out.push(format!("{};", c.add_stmt));
            out.push("GO".to_string());
        }
        out.push(String::new());
    }
    out.push("-- === ADD FOREIGN KEYS ===".to_string());
    for fk in fks {
        out.push(format!("{};", fk.alter_stmt));
        out.push("GO".to_string());
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

    #[allow(clippy::too_many_arguments)]
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
            chunk_key: None,
            chunk_key_indexed: false,
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
    fn col_type_datetime2_scale0_keeps_explicit_zero() {
        // bare `datetime2` == datetime2(7), so scale 0 must be written out
        let col = make_col("X", "datetime2", 6, 0, 0, true, false, None, None);
        assert_eq!(column_type_spec(&col), "datetime2(0)");
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
        assert!(is_excluded(
            "archive",
            "OldOrders",
            &["archive.*".to_string()]
        ));
    }

    // ── Round 1: data/schema fidelity regressions ─────────────────────────────

    #[test]
    fn time_scale_zero_keeps_explicit_scale() {
        // Bare `time` / `datetime2` / `datetimeoffset` mean scale 7; a time(0) column
        // must not be silently widened on import.
        for t in ["time", "datetime2", "datetimeoffset"] {
            let col = make_col("X", t, 8, 0, 0, true, false, None, None);
            assert_eq!(column_type_spec(&col), format!("{t}(0)"));
        }
    }

    #[test]
    fn quote_ident_escapes_closing_bracket() {
        assert_eq!(quote_ident("a]b"), "[a]]b]");
        assert_eq!(qualified_name("s]x", "t"), "[s]]x].[t]");
    }

    #[test]
    fn create_ddl_escapes_odd_identifiers() {
        let col = make_col("we]ird col", "int", 4, 10, 0, true, false, None, None);
        let ddl = generate_create_ddl("my]schema", "t]1", &[col]);
        assert!(ddl.contains("CREATE TABLE [my]]schema].[t]]1] ("), "{ddl}");
        assert!(ddl.contains("[we]]ird col] int NULL"), "{ddl}");
    }

    #[test]
    fn ddl_includes_collation_default_and_computed_columns() {
        let cols = vec![make_col(
            "Name", "varchar", 50, 0, 0, false, false, None, None,
        )];
        let extras = vec![ColumnExtra {
            collation: Some("Latin1_General_100_CI_AS_SC_UTF8".into()),
            default: Some(("DF_t_Name".into(), "('n/a')".into())),
        }];
        let computed = vec![computed_column_def("Upper", "(upper([Name]))", true)];
        let ddl = generate_create_ddl_full("dbo", "t", &cols, &extras, &computed);
        assert!(
            ddl.contains("[Name] varchar(50) COLLATE Latin1_General_100_CI_AS_SC_UTF8 NOT NULL CONSTRAINT [DF_t_Name] DEFAULT ('n/a')"),
            "{ddl}"
        );
        assert!(
            ddl.contains("[Upper] AS (upper([Name])) PERSISTED"),
            "{ddl}"
        );
    }

    #[test]
    fn unsafe_collation_name_is_not_interpolated() {
        let cols = vec![make_col(
            "Name", "varchar", 50, 0, 0, true, false, None, None,
        )];
        let extras = vec![ColumnExtra {
            collation: Some("x; DROP TABLE y".into()),
            default: None,
        }];
        let ddl = generate_create_ddl_full("dbo", "t", &cols, &extras, &[]);
        assert!(!ddl.contains("DROP TABLE"), "{ddl}");
    }

    #[test]
    fn metadata_sql_has_no_line_comments() {
        // `\` continuations in Rust join the statement onto one line, so any `--` comment
        // would comment out the remainder of the query (key_ordinal filter, ORDER BY ...).
        let queries = [
            TABLES_SQL.to_string(),
            columns_sql("[dbo].[T]"),
            computed_columns_sql("[dbo].[T]"),
            clustered_int_pk_sql("[dbo].[T]"),
            fallback_int_col_sql("[dbo].[T]"),
            FKS_SQL.to_string(),
            fk_columns_sql(1),
            KEYS_SQL.to_string(),
            key_columns_sql(1, 1),
            CHECKS_SQL.to_string(),
            SCHEMAS_SQL.to_string(),
            OBJECTS_SQL.to_string(),
        ];
        for q in queries {
            assert!(
                !q.contains("--"),
                "line comment would swallow the rest of: {q}"
            );
            assert!(!q.contains('\n'), "unexpected newline in: {q}");
        }
    }

    #[test]
    fn chunk_key_query_keeps_its_filters() {
        let q = clustered_int_pk_sql("[dbo].[T]");
        assert!(q.contains("ic.key_ordinal = 1"), "{q}");
        assert!(q.contains("tp.name IN ('int', 'bigint')"), "{q}");
        assert!(q.contains("ORDER BY"), "{q}");
    }

    #[test]
    fn object_id_literal_is_unicode_and_escaped() {
        let q = clustered_int_pk_sql("[dbo].[O'Brien]");
        assert!(q.contains("OBJECT_ID(N'[dbo].[O''Brien]')"), "{q}");
    }

    #[test]
    fn columns_query_resolves_alias_types_and_reads_defaults() {
        let q = columns_sql("[dbo].[T]");
        assert!(q.contains("COALESCE(bt.name, tp.name)"), "{q}");
        assert!(q.contains("is_assembly_type = 0"), "{q}");
        assert!(q.contains("sys.default_constraints"), "{q}");
        assert!(q.contains("c.is_computed = 0"), "{q}");
    }

    #[test]
    fn fk_columns_are_looked_up_by_object_id_not_name() {
        let q = fk_columns_sql(12345);
        assert!(q.contains("fk.object_id = 12345"), "{q}");
        assert!(!q.contains("fk.name"), "{q}");
    }

    #[test]
    fn fk_statement_quotes_and_preserves_cascade_rules() {
        let fk = build_fk(
            "FK]x",
            ("s", "Child"),
            ("s", "Parent"),
            &["[a]".into(), "[b]".into()],
            &["[x]".into(), "[y]".into()],
            "CASCADE",
            "SET_NULL",
            false,
            false,
        );
        assert_eq!(
            fk.alter_stmt,
            "ALTER TABLE [s].[Child] WITH CHECK ADD CONSTRAINT [FK]]x] FOREIGN KEY ([a], [b]) \
             REFERENCES [s].[Parent] ([x], [y]) ON DELETE CASCADE ON UPDATE SET NULL"
        );
        assert!(
            fk.drop_stmt
                .contains("IF OBJECT_ID(N'[s].[Child]', N'U') IS NOT NULL ALTER TABLE [s].[Child]"),
            "{}",
            fk.drop_stmt
        );
        assert!(
            fk.drop_stmt.contains("DROP CONSTRAINT IF EXISTS [FK]]x]"),
            "{}",
            fk.drop_stmt
        );
    }

    #[test]
    fn untrusted_or_disabled_fk_is_not_revalidated() {
        let untrusted = build_fk(
            "fk",
            ("s", "C"),
            ("s", "P"),
            &["[a]".into()],
            &["[b]".into()],
            "NO_ACTION",
            "NO_ACTION",
            false,
            true,
        );
        assert!(
            untrusted.alter_stmt.contains("WITH NOCHECK ADD CONSTRAINT"),
            "{}",
            untrusted.alter_stmt
        );
        assert!(
            !untrusted.alter_stmt.contains("NOCHECK CONSTRAINT [fk]"),
            "{}",
            untrusted.alter_stmt
        );
        let disabled = build_fk(
            "fk",
            ("s", "C"),
            ("s", "P"),
            &["[a]".into()],
            &["[b]".into()],
            "NO_ACTION",
            "NO_ACTION",
            true,
            true,
        );
        assert!(
            disabled
                .alter_stmt
                .ends_with("ALTER TABLE [s].[C] NOCHECK CONSTRAINT [fk]"),
            "{}",
            disabled.alter_stmt
        );
    }

    #[test]
    fn key_constraint_ddl_has_order_and_clustering() {
        let c = build_key_constraint(
            "dbo",
            "T",
            "PK_T",
            "PK",
            "CLUSTERED",
            &[("a".into(), false), ("b]".into(), true)],
        );
        assert_eq!(
            c.add_stmt,
            "ALTER TABLE [dbo].[T] ADD CONSTRAINT [PK_T] PRIMARY KEY CLUSTERED ([a] ASC, [b]]] DESC)"
        );
        let u = build_key_constraint(
            "dbo",
            "T",
            "UQ_T",
            "UQ",
            "NONCLUSTERED",
            &[("a".into(), false)],
        );
        assert!(
            u.add_stmt.contains("UNIQUE NONCLUSTERED ([a] ASC)"),
            "{}",
            u.add_stmt
        );
    }

    #[test]
    fn check_constraint_ddl_preserves_trust() {
        let c = build_check_constraint("dbo", "T", "CK", "([a]>(0))", false, true);
        assert_eq!(
            c.add_stmt,
            "ALTER TABLE [dbo].[T] WITH NOCHECK ADD CONSTRAINT [CK] CHECK ([a]>(0))"
        );
    }

    #[test]
    fn schema_sql_creates_schemas_keys_and_orders_sections() {
        let mut t = make_table("sales", "Orders", "[sales].[Orders]");
        t.create_ddl = "CREATE TABLE [sales].[Orders] (x int)".into();
        let key = build_key_constraint(
            "sales",
            "Orders",
            "PK_O",
            "PK",
            "CLUSTERED",
            &[("x".into(), false)],
        );
        let sql = build_schema_sql_full(&["dbo".into(), "sales".into()], &[t], &[], &[key], "");
        let create_schema = sql.find("CREATE SCHEMA").expect("schema created");
        let create_table = sql.find("CREATE TABLE [sales].[Orders]").unwrap();
        let marker = sql.find(POST_DATA_MARKER).unwrap();
        let pk = sql.find("PRIMARY KEY").unwrap();
        assert!(create_schema < create_table, "{sql}");
        assert!(create_table < marker && marker < pk, "{sql}");
        assert!(
            !sql.contains("CREATE SCHEMA [dbo]"),
            "dbo must not be created: {sql}"
        );
        assert_eq!(sql.matches(POST_DATA_MARKER).count(), 1);
    }

    #[test]
    fn generated_schema_with_fks_and_schemas_passes_import_validation() {
        let parent = make_table("sales", "Parent", "[sales].[Parent]");
        let child = make_table("sales", "Child", "[sales].[Child]");
        let fk = build_fk(
            "fk_c_p",
            ("sales", "Child"),
            ("sales", "Parent"),
            &["[a]".into()],
            &["[x]".into()],
            "CASCADE",
            "NO_ACTION",
            false,
            false,
        );
        let sql = build_schema_sql_full(
            &["dbo".into(), "sales".into()],
            &[parent, child],
            &[fk],
            &[],
            "",
        );
        let (pre, post) = crate::import::split_schema_sql(&sql);
        crate::import::validate_ddl_script(pre).unwrap();
        crate::import::validate_ddl_script(post).unwrap();
    }

    #[test]
    fn fks_to_excluded_tables_are_dropped() {
        let tables = vec![make_table("dbo", "A", "[dbo].[A]")];
        let fks = vec![
            make_fk("[dbo].[A]", "[dbo].[Excluded]"),
            make_fk("[dbo].[A]", "[dbo].[A]"),
        ];
        let kept = retain_fks_between(fks, &tables);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].ref_fqn, "[dbo].[A]");
    }

    #[test]
    fn topo_sort_ignores_fks_to_unknown_tables_and_is_deterministic() {
        // B has an FK to a table that is not exported; it must still be emitted in order,
        // not treated as a cycle member.
        let tables = || {
            vec![
                make_table("dbo", "A", "[dbo].[A]"),
                make_table("dbo", "B", "[dbo].[B]"),
                make_table("dbo", "C", "[dbo].[C]"),
            ]
        };
        let fks = vec![make_fk("[dbo].[B]", "[dbo].[Gone]")];
        let first = fqns(&topo_sort(tables(), &fks))
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        assert_eq!(first, ["[dbo].[A]", "[dbo].[B]", "[dbo].[C]"]);
        for _ in 0..20 {
            let again = topo_sort(tables(), &fks);
            assert_eq!(
                fqns(&again),
                first.iter().map(String::as_str).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn topo_sort_cycle_members_all_emitted_once() {
        let tables = vec![
            make_table("dbo", "A", "[dbo].[A]"),
            make_table("dbo", "B", "[dbo].[B]"),
            make_table("dbo", "C", "[dbo].[C]"),
        ];
        // A <-> B cycle, C depends on A
        let fks = vec![
            make_fk("[dbo].[A]", "[dbo].[B]"),
            make_fk("[dbo].[B]", "[dbo].[A]"),
            make_fk("[dbo].[C]", "[dbo].[A]"),
        ];
        let sorted = topo_sort(tables, &fks);
        let mut names: Vec<_> = fqns(&sorted);
        names.sort();
        assert_eq!(names, ["[dbo].[A]", "[dbo].[B]", "[dbo].[C]"]);
    }

    #[test]
    fn topo_sort_multiple_fks_between_same_pair_do_not_strand_the_child() {
        // Two FKs Child -> Parent: in-degree and dependents must stay consistent.
        let tables = vec![
            make_table("dbo", "Child", "[dbo].[Child]"),
            make_table("dbo", "Parent", "[dbo].[Parent]"),
        ];
        let fks = vec![
            make_fk("[dbo].[Child]", "[dbo].[Parent]"),
            make_fk("[dbo].[Child]", "[dbo].[Parent]"),
        ];
        let sorted = topo_sort(tables, &fks);
        assert_eq!(fqns(&sorted), ["[dbo].[Parent]", "[dbo].[Child]"]);
    }
}
