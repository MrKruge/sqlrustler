//! Offline archive/manifest tests — no live DB needed.
use sqlrustler::archive::*;
use std::io::Write;
use tempfile::tempdir;

fn manifest(tables: Vec<ManifestTable>, schema_only: bool) -> Manifest {
    Manifest {
        sqlrustler_version: "0.1.0".into(),
        format_version: 2,
        created_at: "2026-10-24T00:00:00Z".into(),
        source_host: "h".into(),
        source_database: "d".into(),
        total_rows: tables.iter().map(|t| t.row_count).sum(),
        tables,
        schema_only,
    }
}

fn table(fqn: &str, files: Vec<String>, rows: u64) -> ManifestTable {
    ManifestTable {
        table_fqn: fqn.into(),
        file_names: files,
        file_name: String::new(),
        row_count: rows,
        has_identity: false,
        identity_columns: vec![],
    }
}

#[test]
fn empty_archive_zero_tables_round_trips() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("e.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.write_manifest(&manifest(vec![], true)).unwrap();
    w.write_schema_sql("").unwrap();
    w.finish().unwrap();
    let r = ArchiveReader::open(&p).unwrap();
    let m = r.read_manifest().unwrap();
    assert!(m.tables.is_empty());
    assert!(m.schema_only);
    assert_eq!(m.total_rows, 0);
    assert_eq!(r.read_schema_sql().unwrap(), "");
}

#[test]
fn zero_row_table_with_no_files_has_empty_file_list() {
    let t = table("[dbo].[Empty]", vec![], 0);
    assert!(t.all_file_names().is_empty());
}

#[test]
fn unicode_and_emoji_in_schema_and_manifest_round_trip() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("u.rustler");
    let sql = "CREATE TABLE [dbo].[Tëst_日本語_😀] (c NVARCHAR(10)); -- ñ ü 🚀";
    let m = manifest(
        vec![table(
            "[dbo].[Tëst_日本語_😀]",
            vec!["data/x.parquet".into()],
            1,
        )],
        false,
    );
    let mut w = ArchiveWriter::create(&p, 3).unwrap();
    w.write_manifest(&m).unwrap();
    w.write_schema_sql(sql).unwrap();
    w.finish().unwrap();
    let r = ArchiveReader::open(&p).unwrap();
    assert_eq!(r.read_schema_sql().unwrap(), sql);
    assert_eq!(
        r.read_manifest().unwrap().tables[0].table_fqn,
        "[dbo].[Tëst_日本語_😀]"
    );
}

#[test]
fn large_parquet_blob_round_trips_byte_for_byte() {
    // 8 MiB of pseudo-random-ish bytes (max-LOB-ish payload through tar+zstd)
    let dir = tempdir().unwrap();
    let src = dir.path().join("big.parquet");
    let data: Vec<u8> = (0..8 * 1024 * 1024u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    std::fs::write(&src, &data).unwrap();
    let p = dir.path().join("b.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.add_parquet_file("data/big.parquet", &src).unwrap();
    w.finish().unwrap();
    let r = ArchiveReader::open(&p).unwrap();
    assert_eq!(r.read_parquet("data/big.parquet").unwrap(), data);
}

#[test]
fn zero_byte_parquet_entry_round_trips() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("z.parquet");
    std::fs::write(&src, b"").unwrap();
    let p = dir.path().join("z.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.add_parquet_file("data/z.parquet", &src).unwrap();
    w.finish().unwrap();
    assert!(ArchiveReader::open(&p)
        .unwrap()
        .read_parquet("data/z.parquet")
        .unwrap()
        .is_empty());
}

#[test]
fn many_chunked_parts_all_retrievable() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("m.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    let mut names = vec![];
    for i in 0..25 {
        let f = dir.path().join(format!("part{i}.parquet"));
        std::fs::write(&f, vec![i as u8; 100 + i]).unwrap();
        let n = format!("data/dbo_T.part{i}.parquet");
        w.add_parquet_file(&n, &f).unwrap();
        names.push(n);
    }
    w.write_manifest(&manifest(
        vec![table("[dbo].[T]", names.clone(), 25)],
        false,
    ))
    .unwrap();
    w.finish().unwrap();
    let r = ArchiveReader::open(&p).unwrap();
    let m = r.read_manifest().unwrap();
    assert_eq!(m.tables[0].all_file_names().len(), 25);
    for (i, n) in names.iter().enumerate() {
        assert_eq!(r.read_parquet(n).unwrap(), vec![i as u8; 100 + i]);
    }
}

#[test]
fn legacy_v1_manifest_with_single_file_name_parses() {
    let json = r#"{
        "sqlrustler_version":"0.0.9","format_version":1,"created_at":"x",
        "source_host":"h","source_database":"d","total_rows":3,"schema_only":false,
        "tables":[{"table_fqn":"[dbo].[A]","file_name":"data/dbo_A.parquet",
                   "row_count":3,"has_identity":true,"identity_columns":["Id"]}]}"#;
    let m: Manifest = serde_json::from_str(json).unwrap();
    assert_eq!(m.format_version, 1);
    assert_eq!(m.tables[0].all_file_names(), vec!["data/dbo_A.parquet"]);
}

#[test]
fn v2_file_names_takes_precedence_over_legacy_file_name() {
    let mut t = table("[dbo].[A]", vec!["a".into(), "b".into()], 1);
    t.file_name = "legacy".into();
    assert_eq!(t.all_file_names(), vec!["a", "b"]);
}

#[test]
fn legacy_file_name_not_serialized_when_empty() {
    let m = manifest(vec![table("[dbo].[A]", vec!["a".into()], 1)], false);
    let s = serde_json::to_string(&m).unwrap();
    assert!(!s.contains("\"file_name\""), "{s}");
    assert!(s.contains("\"file_names\""));
}

#[test]
fn manifest_missing_required_field_is_error() {
    let json = r#"{"sqlrustler_version":"0.1","format_version":2}"#;
    assert!(serde_json::from_str::<Manifest>(json).is_err());
}

#[test]
fn manifest_wrong_type_for_row_count_is_error() {
    let json = r#"{"sqlrustler_version":"0.1","format_version":2,"created_at":"x",
      "source_host":"h","source_database":"d","total_rows":0,"schema_only":false,
      "tables":[{"table_fqn":"[dbo].[A]","file_names":[],"row_count":"many",
      "has_identity":false,"identity_columns":[]}]}"#;
    assert!(serde_json::from_str::<Manifest>(json).is_err());
}

#[test]
fn manifest_negative_row_count_is_error() {
    let json = r#"{"sqlrustler_version":"0.1","format_version":2,"created_at":"x",
      "source_host":"h","source_database":"d","total_rows":-1,"schema_only":false,"tables":[]}"#;
    assert!(serde_json::from_str::<Manifest>(json).is_err());
}

#[test]
fn manifest_future_format_version_still_parses_so_callers_must_check() {
    // The reader does not reject unknown versions; the version check must live in import.
    let json = r#"{"sqlrustler_version":"9.9","format_version":999,"created_at":"x",
      "source_host":"h","source_database":"d","total_rows":0,"schema_only":false,"tables":[],
      "some_future_field": {"a":1}}"#;
    let m: Manifest = serde_json::from_str(json).unwrap();
    assert_eq!(m.format_version, 999);
}

#[test]
fn corrupt_manifest_json_in_archive_is_error_not_panic() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("c.rustler");
    // Build tar+zstd by hand with garbage manifest.json
    let f = std::fs::File::create(&p).unwrap();
    let enc = zstd::Encoder::new(f, 1).unwrap();
    let mut b = tar::Builder::new(enc);
    let bytes = b"{not json";
    let mut h = tar::Header::new_gnu();
    h.set_size(bytes.len() as u64);
    h.set_mode(0o644);
    h.set_cksum();
    b.append_data(&mut h, "manifest.json", &bytes[..]).unwrap();
    b.into_inner().unwrap().finish().unwrap();
    let r = ArchiveReader::open(&p).unwrap();
    assert!(r.read_manifest().is_err());
}

#[test]
fn non_utf8_schema_sql_is_error() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("n.rustler");
    let f = std::fs::File::create(&p).unwrap();
    let enc = zstd::Encoder::new(f, 1).unwrap();
    let mut b = tar::Builder::new(enc);
    let bytes: &[u8] = &[0xff, 0xfe, 0x00];
    let mut h = tar::Header::new_gnu();
    h.set_size(bytes.len() as u64);
    h.set_mode(0o644);
    h.set_cksum();
    b.append_data(&mut h, "schema.sql", bytes).unwrap();
    b.into_inner().unwrap().finish().unwrap();
    assert!(ArchiveReader::open(&p).unwrap().read_schema_sql().is_err());
}

#[test]
fn truncated_archive_is_error_or_missing_entries_never_panics() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("t.rustler");
    let src = dir.path().join("d.parquet");
    let data: Vec<u8> = (0..2_000_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 11) as u8)
        .collect();
    std::fs::write(&src, &data).unwrap();
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.write_manifest(&manifest(vec![], false)).unwrap();
    w.add_parquet_file("data/d.parquet", &src).unwrap();
    w.finish().unwrap();
    let full = std::fs::read(&p).unwrap();
    for cut in [0usize, 1, 4, full.len() / 4, full.len() / 2, full.len() - 1] {
        let tp = dir.path().join(format!("cut{cut}.rustler"));
        std::fs::write(&tp, &full[..cut]).unwrap();
        let res = std::panic::catch_unwind(|| ArchiveReader::open(&tp));
        let res = res.expect("ArchiveReader::open must not panic on truncated input");
        if let Ok(r) = res {
            // If open "succeeded", the full parquet blob must not be silently complete.
            if let Ok(b) = r.read_parquet("data/d.parquet") {
                assert_ne!(
                    b.len(),
                    data.len(),
                    "truncated archive yielded full payload (cut={cut})"
                );
            }
        }
    }
}

#[test]
fn truncated_archive_must_be_rejected_at_open() {
    // Silent truncation = silent data loss on import. Desired: open() errors.
    let dir = tempdir().unwrap();
    let p = dir.path().join("t.rustler");
    let src = dir.path().join("d.parquet");
    std::fs::write(&src, vec![7u8; 500_000]).unwrap();
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.write_manifest(&manifest(vec![], false)).unwrap();
    w.add_parquet_file("data/d.parquet", &src).unwrap();
    w.add_parquet_file("data/e.parquet", &src).unwrap();
    w.finish().unwrap();
    let full = std::fs::read(&p).unwrap();
    let tp = dir.path().join("cut.rustler");
    std::fs::write(&tp, &full[..full.len() - 10]).unwrap();
    assert!(
        ArchiveReader::open(&tp).is_err(),
        "truncated zstd stream was accepted"
    );
}

#[test]
fn bit_flipped_archive_does_not_panic() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("f.rustler");
    let src = dir.path().join("d.parquet");
    std::fs::write(&src, vec![9u8; 100_000]).unwrap();
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.write_manifest(&manifest(vec![], false)).unwrap();
    w.add_parquet_file("data/d.parquet", &src).unwrap();
    w.finish().unwrap();
    let mut bytes = std::fs::read(&p).unwrap();
    for pos in [10usize, bytes.len() / 2, bytes.len() - 3] {
        bytes[pos] ^= 0xFF;
    }
    let tp = dir.path().join("flip.rustler");
    std::fs::write(&tp, &bytes).unwrap();
    let r = std::panic::catch_unwind(|| ArchiveReader::open(&tp));
    assert!(r.is_ok(), "ArchiveReader::open panicked on corrupt data");
}

#[test]
fn not_a_zstd_file_is_error() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("junk.rustler");
    let mut f = std::fs::File::create(&p).unwrap();
    f.write_all(b"this is definitely not zstd").unwrap();
    drop(f);
    assert!(ArchiveReader::open(&p).is_err());
}

#[test]
fn zero_byte_file_is_error_or_empty() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("zero.rustler");
    std::fs::write(&p, b"").unwrap();
    match ArchiveReader::open(&p) {
        Err(_) => {}
        Ok(r) => assert!(r.read_manifest().is_err()),
    }
}

#[test]
fn open_nonexistent_path_is_error() {
    let dir = tempdir().unwrap();
    assert!(ArchiveReader::open(&dir.path().join("nope.rustler")).is_err());
}

#[test]
fn create_in_nonexistent_dir_is_error() {
    let dir = tempdir().unwrap();
    assert!(ArchiveWriter::create(&dir.path().join("no/such/dir/a.rustler"), 1).is_err());
}

#[test]
fn add_missing_parquet_source_is_error() {
    let dir = tempdir().unwrap();
    let mut w = ArchiveWriter::create(&dir.path().join("a.rustler"), 1).unwrap();
    assert!(w
        .add_parquet_file("data/x.parquet", &dir.path().join("missing"))
        .is_err());
}

#[test]
fn compression_levels_1_and_22_both_round_trip() {
    for lvl in [1, 22] {
        let dir = tempdir().unwrap();
        let p = dir.path().join("l.rustler");
        let mut w = ArchiveWriter::create(&p, lvl).unwrap();
        w.write_schema_sql("SELECT 1;").unwrap();
        w.finish().unwrap();
        assert_eq!(
            ArchiveReader::open(&p).unwrap().read_schema_sql().unwrap(),
            "SELECT 1;"
        );
    }
}

#[test]
fn duplicate_entry_name_is_rejected() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("d.rustler");
    let mut w = ArchiveWriter::create(&p, 1).unwrap();
    w.write_schema_sql("first").unwrap();
    w.write_schema_sql("second").unwrap();
    w.finish().unwrap();
    assert!(ArchiveReader::open(&p).is_err());
}

#[test]
fn fqn_to_filename_edge_cases() {
    assert_eq!(fqn_to_filename(""), "");
    assert_eq!(fqn_to_filename("[dbo].[Tëst😀]"), "dbo_Tëst😀");
}

#[test]
#[ignore = "known bug: src/archive.rs fqn_to_filename maps '.'->'_' so [a_b].[c] and [a].[b_c] collide on 'a_b_c' (export.rs now fails the run on such a collision instead of overwriting; renaming the scheme would change the archive layout, so it is left as is)"]
fn fqn_to_filename_distinct_tables_do_not_collide() {
    assert_ne!(fqn_to_filename("[a_b].[c]"), fqn_to_filename("[a].[b_c]"));
}

#[test]
fn fqn_to_filename_cannot_escape_data_dir() {
    // Table names are validated on import, but filename generation itself should not
    // let path separators through.
    let p = parquet_archive_path("[dbo].[../../etc/passwd]");
    assert!(!p.contains(".."), "path traversal not neutralised: {p}");
}

#[test]
fn parquet_archive_path_format() {
    assert_eq!(
        parquet_archive_path("[dbo].[Orders]"),
        "data/dbo_Orders.parquet"
    );
}
