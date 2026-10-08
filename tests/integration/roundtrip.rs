//! Integration tests — require a live Azure SQL Database.
//!
//! Set these env vars to run:
//!   SQLRUSTLER_TEST_HOST     SQL Server hostname
//!   SQLRUSTLER_TEST_DB       Database name
//!   SQLRUSTLER_TEST_USER     SQL login username
//!   SQLRUSTLER_TEST_PASSWORD SQL login password
//!
//! Tests are automatically skipped when the env vars are not set.

use std::env;
use tempfile::NamedTempFile;

fn test_conn() -> Option<sqlrustler::cli::ConnectArgs> {
    let host = env::var("SQLRUSTLER_TEST_HOST").ok()?;
    let database = env::var("SQLRUSTLER_TEST_DB").ok()?;
    let user = env::var("SQLRUSTLER_TEST_USER").ok();
    let password = env::var("SQLRUSTLER_TEST_PASSWORD").ok();
    Some(sqlrustler::cli::ConnectArgs {
        host,
        database,
        user,
        password,
        aad_token: None,
        port: 1433,
        trust_cert: false,
    })
}

#[tokio::test]
async fn test_export_produces_valid_archive() {
    let Some(conn) = test_conn() else {
        eprintln!("Skipping: SQLRUSTLER_TEST_* env vars not set");
        return;
    };

    let tmp = NamedTempFile::new().unwrap();
    let args = sqlrustler::cli::ExportArgs {
        conn,
        output: Some(tmp.path().to_path_buf()),
        parallel: 2,
        batch_size: 1000,
        exclude_tables: vec![],
        schema_only: false,
        compression_level: 1,
    };

    sqlrustler::export::run(args).await.expect("export failed");

    let archive = sqlrustler::archive::ArchiveReader::open(tmp.path())
        .expect("failed to open archive");
    let manifest = archive.read_manifest().expect("failed to read manifest");

    assert_eq!(manifest.format_version, 1);
    assert!(!manifest.tables.is_empty(), "archive should contain at least one table");
    assert!(archive.read_schema_sql().is_ok(), "schema.sql should be present");
}

#[tokio::test]
async fn test_schema_only_has_no_parquet_data() {
    let Some(conn) = test_conn() else {
        eprintln!("Skipping: SQLRUSTLER_TEST_* env vars not set");
        return;
    };

    let tmp = NamedTempFile::new().unwrap();
    let args = sqlrustler::cli::ExportArgs {
        conn,
        output: Some(tmp.path().to_path_buf()),
        parallel: 1,
        batch_size: 1000,
        exclude_tables: vec![],
        schema_only: true,
        compression_level: 1,
    };

    sqlrustler::export::run(args).await.expect("schema-only export failed");

    let archive = sqlrustler::archive::ArchiveReader::open(tmp.path()).unwrap();
    let manifest = archive.read_manifest().unwrap();

    for table in &manifest.tables {
        assert_eq!(
            table.row_count, 0,
            "schema-only export should have 0 rows for {}",
            table.table_fqn
        );
    }
}

#[tokio::test]
async fn test_roundtrip_row_counts_match() {
    let Some(conn) = test_conn() else {
        eprintln!("Skipping: SQLRUSTLER_TEST_* env vars not set");
        return;
    };

    // Step 1: export
    let export_tmp = NamedTempFile::new().unwrap();
    let export_args = sqlrustler::cli::ExportArgs {
        conn: conn.clone(),
        output: Some(export_tmp.path().to_path_buf()),
        parallel: 2,
        batch_size: 1000,
        exclude_tables: vec![],
        schema_only: false,
        compression_level: 1,
    };

    sqlrustler::export::run(export_args).await.expect("export failed");

    // Read manifest for expected row counts
    let archive = sqlrustler::archive::ArchiveReader::open(export_tmp.path()).unwrap();
    let manifest = archive.read_manifest().unwrap();

    // Step 2: import into a fresh database
    let target_db = format!(
        "{}_sqlrustler_test_{}",
        conn.database,
        chrono::Utc::now().timestamp()
    );
    let import_conn = sqlrustler::cli::ConnectArgs {
        database: target_db.clone(),
        ..conn.clone()
    };

    let import_args = sqlrustler::cli::ImportArgs {
        conn: import_conn.clone(),
        input: export_tmp.path().to_path_buf(),
        parallel: 2,
        batch_size: 500,
        data_only: false,
        drop_existing: true,
    };

    sqlrustler::import::run(import_args).await.expect("import failed");

    // Step 3: verify row counts in target DB
    let target_pool = sqlrustler::connection::build_pool(&import_conn, 2)
        .await
        .expect("failed to connect to target DB");

    for table in &manifest.tables {
        let mut conn = target_pool.get().await.unwrap();
        let sql = format!("SELECT COUNT(*) FROM {}", table.table_fqn);
        let rows = conn.query(&sql, &[]).await.unwrap().into_results().await.unwrap();
        let count: i32 = rows.into_iter().flatten().next()
            .and_then(|r| r.get(0))
            .unwrap_or(0);

        assert_eq!(
            count as u64, table.row_count,
            "Row count mismatch for {}: expected {}, got {}",
            table.table_fqn, table.row_count, count
        );
    }
}
