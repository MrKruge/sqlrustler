//! Round-2 offline tests: hostile/inconsistent archive contents, manifest invariants,
//! connection-arg edge cases. No DB.
use clap::Parser;
use sqlrustler::archive::*;
use sqlrustler::cli::*;
use tempfile::tempdir;

fn raw_archive(path: &std::path::Path, entries: &[(&str, &[u8])]) {
    let f = std::fs::File::create(path).unwrap();
    let enc = zstd::Encoder::new(f, 1).unwrap();
    let mut b = tar::Builder::new(enc);
    for (name, data) in entries {
        let mut h = tar::Header::new_gnu();
        h.set_size(data.len() as u64);
        h.set_mode(0o644);
        h.set_cksum();
        b.append_data(&mut h, name, *data).unwrap();
    }
    b.into_inner().unwrap().finish().unwrap();
}

fn m(json_tables: &str, total: i64) -> String {
    format!(
        r#"{{"sqlrustler_version":"0.1","format_version":2,"created_at":"x","source_host":"h",
      "source_database":"d","total_rows":{total},"schema_only":false,"tables":{json_tables}}}"#
    )
}

#[test]
fn reader_does_not_validate_total_rows_against_table_sum() {
    // Documents: the reader accepts inconsistent totals; import must not trust total_rows.
    let t = r#"[{"table_fqn":"[dbo].[A]","file_names":[],"row_count":5,"has_identity":false,"identity_columns":[]}]"#;
    let dir = tempdir().unwrap();
    let p = dir.path().join("a.rustler");
    raw_archive(&p, &[("manifest.json", m(t, 999).as_bytes())]);
    let man = ArchiveReader::open(&p).unwrap().read_manifest().unwrap();
    assert_eq!(man.total_rows, 999);
    assert_eq!(man.tables.iter().map(|t| t.row_count).sum::<u64>(), 5);
}

#[test]
fn manifest_with_hostile_table_fqn_parses_so_import_must_validate() {
    // Parsing must succeed (validation is import's job via validate_fqn); content preserved verbatim.
    let evil = "[dbo].[x]; DROP DATABASE master;--]";
    let t = format!(
        r#"[{{"table_fqn":{},"file_names":["data/x.parquet"],"row_count":1,"has_identity":false,"identity_columns":[]}}]"#,
        serde_json::to_string(evil).unwrap()
    );
    let dir = tempdir().unwrap();
    let p = dir.path().join("e.rustler");
    raw_archive(&p, &[("manifest.json", m(&t, 1).as_bytes())]);
    let man = ArchiveReader::open(&p).unwrap().read_manifest().unwrap();
    assert_eq!(man.tables[0].table_fqn, evil);
}

#[test]
fn manifest_file_names_with_traversal_are_only_lookup_keys() {
    // read_parquet is a HashMap lookup, never touches the filesystem -> traversal strings are inert.
    let dir = tempdir().unwrap();
    let p = dir.path().join("t.rustler");
    raw_archive(&p, &[("data/ok.parquet", b"PAR1")]);
    let r = ArchiveReader::open(&p).unwrap();
    assert!(r.read_parquet("../../etc/passwd").is_err());
    assert!(r.read_parquet("/etc/passwd").is_err());
    assert_eq!(r.read_parquet("data/ok.parquet").unwrap(), b"PAR1");
}

#[test]
fn archive_entry_with_dot_slash_prefix_is_found_by_normalised_name() {
    // Archives re-packed with `tar` often use "./manifest.json".
    let dir = tempdir().unwrap();
    let p = dir.path().join("dot.rustler");
    let t = "[]";
    raw_archive(&p, &[("./manifest.json", m(t, 0).as_bytes())]);
    let r = ArchiveReader::open(&p).unwrap();
    assert!(
        r.read_manifest().is_ok(),
        "archives with ./ prefix should still be readable"
    );
}

#[test]
fn manifest_row_count_u64_max_round_trips() {
    let man = Manifest {
        sqlrustler_version: "v".into(),
        format_version: 2,
        created_at: "c".into(),
        source_host: "h".into(),
        source_database: "d".into(),
        tables: vec![ManifestTable {
            table_fqn: "[dbo].[A]".into(),
            file_names: vec!["f".into()],
            file_name: String::new(),
            row_count: u64::MAX,
            has_identity: false,
            identity_columns: vec![],
        }],
        total_rows: u64::MAX,
        schema_only: false,
    };
    let dir = tempdir().unwrap();
    let p = dir.path().join("m.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.write_manifest(&man).unwrap();
    w.finish().unwrap();
    let got = ArchiveReader::open(&p).unwrap().read_manifest().unwrap();
    assert_eq!(got.total_rows, u64::MAX);
    assert_eq!(got.tables[0].row_count, u64::MAX);
}

#[test]
fn multiple_identity_columns_and_duplicate_tables_preserved() {
    let t = ManifestTable {
        table_fqn: "[dbo].[A]".into(),
        file_names: vec![],
        file_name: String::new(),
        row_count: 0,
        has_identity: true,
        identity_columns: vec!["a".into(), "b".into()],
    };
    let man = Manifest {
        sqlrustler_version: "v".into(),
        format_version: 2,
        created_at: "c".into(),
        source_host: "h".into(),
        source_database: "d".into(),
        tables: vec![t.clone(), t],
        total_rows: 0,
        schema_only: false,
    };
    let s = serde_json::to_string(&man).unwrap();
    let back: Manifest = serde_json::from_str(&s).unwrap();
    assert_eq!(back.tables.len(), 2);
    assert_eq!(back.tables[0].identity_columns, vec!["a", "b"]);
}

#[test]
fn schema_only_manifest_has_no_files_and_zero_rows() {
    let t = r#"[{"table_fqn":"[dbo].[A]","file_names":[],"row_count":0,"has_identity":false,"identity_columns":[]}]"#;
    let man: Manifest =
        serde_json::from_str(&m(t, 0).replace("\"schema_only\":false", "\"schema_only\":true"))
            .unwrap();
    assert!(man.schema_only);
    assert!(man.tables[0].all_file_names().is_empty());
}

#[test]
fn schema_sql_with_post_data_marker_and_go_survives_archive() {
    let sql = format!("CREATE TABLE [dbo].[A](id INT);\nGO\n{}\nALTER TABLE [dbo].[A] ADD CONSTRAINT pk PRIMARY KEY(id);\nGO\n",
        sqlrustler::schema::POST_DATA_MARKER);
    let dir = tempdir().unwrap();
    let p = dir.path().join("s.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.write_schema_sql(&sql).unwrap();
    w.finish().unwrap();
    let got = ArchiveReader::open(&p).unwrap().read_schema_sql().unwrap();
    assert_eq!(got, sql);
    assert_eq!(got.matches(sqlrustler::schema::POST_DATA_MARKER).count(), 1);
}

#[test]
fn schema_sql_over_one_mib_round_trips() {
    let sql: String = (0..20_000)
        .map(|i| format!("CREATE VIEW [dbo].[v{i}] AS SELECT {i};\nGO\n"))
        .collect();
    assert!(sql.len() > 700_000);
    let dir = tempdir().unwrap();
    let p = dir.path().join("big.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.write_schema_sql(&sql).unwrap();
    w.finish().unwrap();
    assert_eq!(
        ArchiveReader::open(&p).unwrap().read_schema_sql().unwrap(),
        sql
    );
}

#[test]
fn file_name_with_long_path_over_100_bytes_round_trips() {
    // tar's classic header caps names at 100 bytes; fqn_to_filename can exceed it for long names.
    let fqn = format!("[dbo].[{}]", "T".repeat(200));
    let name = parquet_archive_path(&fqn);
    assert!(name.len() > 100);
    let dir = tempdir().unwrap();
    let src = dir.path().join("s.parquet");
    std::fs::write(&src, b"PAR1xyz").unwrap();
    let p = dir.path().join("l.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.add_parquet_file(&name, &src).unwrap();
    w.finish().unwrap();
    assert_eq!(
        ArchiveReader::open(&p)
            .unwrap()
            .read_parquet(&name)
            .unwrap(),
        b"PAR1xyz"
    );
}

#[test]
fn unicode_table_name_data_path_round_trips() {
    let name = parquet_archive_path("[dbo].[Café_日本_😀]");
    let dir = tempdir().unwrap();
    let src = dir.path().join("s.parquet");
    std::fs::write(&src, b"X").unwrap();
    let p = dir.path().join("u.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.add_parquet_file(&name, &src).unwrap();
    w.finish().unwrap();
    assert_eq!(
        ArchiveReader::open(&p)
            .unwrap()
            .read_parquet(&name)
            .unwrap(),
        b"X"
    );
}

#[test]
fn unfinished_writer_dropped_without_finish_is_not_a_valid_archive() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("unfinished.rustler");
    {
        let mut w = ArchiveWriter::create(&p, 1).unwrap();
        w.write_schema_sql("CREATE TABLE x(i int);").unwrap();
        // dropped without finish() (e.g. export aborted mid-way)
    }
    // Must not be silently accepted as a complete archive.
    match std::panic::catch_unwind(|| ArchiveReader::open(&p)) {
        Ok(Ok(r)) => assert!(r.read_manifest().is_err(), "no manifest was written"),
        Ok(Err(_)) => {}
        Err(_) => panic!("reader panicked on unfinished archive"),
    }
}

// ── Connection / CLI edge cases ───────────────────────────────────────────────

fn conn(args: &[&str]) -> ConnectArgs {
    let mut v = vec!["sqlrustler", "test", "-H", "h", "-d", "d"];
    v.extend_from_slice(args);
    match Cli::try_parse_from(v).unwrap().command {
        Command::Test(t) => t.conn,
        _ => unreachable!(),
    }
}

#[test]
fn connect_args_debug_with_no_secrets_shows_none() {
    let c = conn(&[]);
    let d = format!("{c:?}");
    assert!(d.contains("password: None"), "{d}");
    assert!(d.contains("aad_token: None"), "{d}");
}

#[test]
fn connect_args_clone_preserves_fields_and_redaction() {
    let c = conn(&[
        "-p",
        "pw-SECRET",
        "--port",
        "14330",
        "--trust-cert",
        "--azure-auth",
    ]);
    let c2 = c.clone();
    assert_eq!(c2.port, 14330);
    assert!(c2.trust_cert && c2.azure_auth);
    assert!(!format!("{c2:?}").contains("pw-SECRET"));
}

#[test]
fn password_with_special_characters_is_preserved_verbatim() {
    let c = conn(&["-p", "p@ss;w=rd'\"\\ ñ😀"]);
    assert_eq!(c.password.as_deref(), Some("p@ss;w=rd'\"\\ ñ😀"));
}

#[test]
fn password_not_leaked_via_top_level_cli_debug() {
    let cli = Cli::try_parse_from([
        "sqlrustler",
        "export",
        "-H",
        "h",
        "-d",
        "d",
        "-p",
        "TOPSECRET-xyz",
    ])
    .unwrap();
    assert!(!format!("{cli:?}").contains("TOPSECRET-xyz"));
}

#[test]
fn port_zero_and_boundary_parse() {
    assert_eq!(conn(&["--port", "65535"]).port, 65535);
    // port 0 is invalid for a TCP connect; ideally rejected at parse time.
    let r = Cli::try_parse_from(["sqlrustler", "test", "-H", "h", "-d", "d", "--port", "0"]);
    if r.is_ok() {
        eprintln!("NOTE: --port 0 accepted by clap (connection will fail later)");
    }
}

#[test]
fn compression_level_out_of_range_is_not_rejected_at_parse() {
    // Documents: --compression-level is an unchecked i32 at parse time (writer clamps to 1..=22 in parquet,
    // but ArchiveWriter::create passes it straight to zstd).
    let c = Cli::try_parse_from([
        "sqlrustler",
        "export",
        "-H",
        "h",
        "-d",
        "d",
        "--compression-level",
        "99",
    ]);
    assert!(c.is_ok());
}

#[test]
fn archive_writer_extreme_compression_levels_do_not_panic() {
    let dir = tempdir().unwrap();
    for lvl in [-5, 0, 22, 99, i32::MIN] {
        let p = dir.path().join(format!("lvl{}.rustler", lvl));
        let r = std::panic::catch_unwind(|| {
            if let Ok(mut w) = ArchiveWriter::create(&p, lvl) {
                let _ = w.write_schema_sql("x");
                let _ = w.finish();
            }
        });
        assert!(r.is_ok(), "panic at compression level {lvl}");
    }
}

#[test]
fn import_must_check_format_version_before_use() {
    // A format_version newer than this binary understands must be detectable via the public API.
    let t = "[]";
    let dir = tempdir().unwrap();
    let p = dir.path().join("v.rustler");
    raw_archive(
        &p,
        &[
            (
                "manifest.json",
                m(t, 0)
                    .replace("\"format_version\":2", "\"format_version\":99")
                    .as_bytes(),
            ),
            ("schema.sql", b""),
        ],
    );
    let man = ArchiveReader::open(&p).unwrap().read_manifest().unwrap();
    assert_eq!(man.format_version, 99);
    // No public const exists for the max supported version, and import.rs:25-29 only logs it.
    // See report: unsupported format versions are not rejected.
}
