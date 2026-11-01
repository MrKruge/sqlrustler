//! CLI parsing tests via clap try_parse_from — no DB, no network.
use clap::Parser;
use sqlrustler::cli::*;

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    let mut v = vec!["sqlrustler"];
    v.extend_from_slice(args);
    Cli::try_parse_from(v)
}

fn export(args: &[&str]) -> ExportArgs {
    let mut v = vec!["export", "-H", "h", "-d", "db"];
    v.extend_from_slice(args);
    match parse(&v).unwrap().command {
        Command::Export(e) => e,
        other => panic!("expected export, got {other:?}"),
    }
}

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn no_env() -> std::sync::MutexGuard<'static, ()> {
    let g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for k in [
        "SQLRUSTLER_HOST",
        "SQLRUSTLER_DATABASE",
        "SQLRUSTLER_USER",
        "SQLRUSTLER_PASSWORD",
        "SQLRUSTLER_AAD_TOKEN",
    ] {
        std::env::remove_var(k);
    }
    g
}

#[test]
fn export_defaults() {
    let _g = no_env();
    let e = export(&[]);
    assert_eq!(e.conn.host, "h");
    assert_eq!(e.conn.database, "db");
    assert_eq!(e.conn.port, 1433);
    assert!(!e.conn.trust_cert && !e.conn.azure_auth);
    assert_eq!(e.batch_size, 10_000);
    assert_eq!(e.compression_level, 3);
    assert!(!e.schema_only && !e.validate && !e.monitor);
    assert!(e.exclude_tables.is_empty());
    assert!(e.output.is_none());
    assert!(e.parallel >= 1);
    assert!(e.table_parallelism >= 1);
}

#[test]
fn missing_host_is_clap_error_not_panic() {
    let _g = no_env();
    let err = parse(&["export", "-d", "db"]).unwrap_err();
    assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
}

#[test]
fn missing_database_is_error() {
    let _g = no_env();
    assert!(parse(&["export", "-H", "h"]).is_err());
}

#[test]
fn import_requires_input() {
    let _g = no_env();
    assert!(parse(&["import", "-H", "h", "-d", "db"]).is_err());
}

#[test]
fn no_subcommand_is_error() {
    assert!(parse(&[]).is_err());
}

#[test]
fn unknown_subcommand_and_flag_are_errors() {
    assert!(parse(&["frobnicate"]).is_err());
    assert!(parse(&["export", "-H", "h", "-d", "db", "--no-such-flag"]).is_err());
}

#[test]
fn import_flags_parse() {
    let _g = no_env();
    let c = parse(&[
        "import",
        "-H",
        "h",
        "-d",
        "db",
        "-i",
        "a.rustler",
        "--data-only",
        "--drop-existing",
        "-j",
        "3",
        "--batch-size",
        "7",
    ])
    .unwrap();
    let Command::Import(i) = c.command else {
        panic!()
    };
    assert!(i.data_only && i.drop_existing);
    assert_eq!(i.parallel, 3);
    assert_eq!(i.batch_size, 7);
    assert_eq!(i.input.to_str().unwrap(), "a.rustler");
}

#[test]
fn import_defaults() {
    let _g = no_env();
    let Command::Import(i) = parse(&["import", "-H", "h", "-d", "db", "-i", "x"])
        .unwrap()
        .command
    else {
        panic!()
    };
    assert_eq!(i.batch_size, 1000);
    assert!(!i.data_only && !i.drop_existing);
}

#[test]
fn exclude_tables_comma_split() {
    let _g = no_env();
    let e = export(&["--exclude-tables", "tmp_*,log_*,x"]);
    assert_eq!(e.exclude_tables, vec!["tmp_*", "log_*", "x"]);
}

#[test]
fn exclude_tables_repeatable_flag_accumulates() {
    let _g = no_env();
    let e = export(&["--exclude-tables", "a", "--exclude-tables", "b"]);
    assert_eq!(e.exclude_tables, vec!["a", "b"]);
}

#[test]
fn pressure_presets_validate_and_apply() {
    let _g = no_env();
    assert_eq!(
        export(&["--pressure", "light"]).effective_parallelism(),
        (4, 10_000)
    );
    assert_eq!(
        export(&["--pressure", "medium"]).effective_parallelism(),
        (8, 50_000)
    );
    let (p, b) = export(&["--pressure", "full"]).effective_parallelism();
    assert!(p >= 8 && b >= 50_000);
    assert!(parse(&["export", "-H", "h", "-d", "db", "--pressure", "ludicrous"]).is_err());
}

#[test]
fn no_pressure_uses_explicit_values() {
    let _g = no_env();
    let e = export(&["-j", "5", "--batch-size", "123"]);
    assert_eq!(e.effective_parallelism(), (5, 123));
}

#[test]
fn table_parallelism_full_preset_floor() {
    let _g = no_env();
    assert!(
        export(&["--pressure", "full", "--table-parallelism", "1"]).effective_table_parallelism()
            >= 4
    );
    assert_eq!(
        export(&["--table-parallelism", "2"]).effective_table_parallelism(),
        2
    );
}

#[test]
fn numeric_flags_reject_garbage_and_negative() {
    let _g = no_env();
    assert!(parse(&["export", "-H", "h", "-d", "db", "-j", "abc"]).is_err());
    assert!(parse(&["export", "-H", "h", "-d", "db", "-j", "-1"]).is_err());
    assert!(parse(&["export", "-H", "h", "-d", "db", "--port", "70000"]).is_err());
}

#[test]
fn zero_parallel_and_batch_rejected() {
    let _g = no_env();
    assert!(parse(&["export", "-H", "h", "-d", "db", "-j", "0"]).is_err());
    assert!(parse(&["export", "-H", "h", "-d", "db", "--batch-size", "0"]).is_err());
}

#[test]
fn zero_parallel_current_behaviour_documented() {
    let _g = no_env();
    // Documents current behaviour so a change is noticed: 0 is accepted by clap.
    if let Ok(c) = parse(&["export", "-H", "h", "-d", "db", "-j", "0"]) {
        let Command::Export(e) = c.command else {
            panic!()
        };
        assert_eq!(e.parallel, 0);
    }
}

#[test]
fn debug_output_redacts_secrets() {
    let _g = no_env();
    let e = export(&[
        "-u",
        "bob",
        "-p",
        "hunter2-SECRET",
        "--aad-token",
        "tok-SECRET-123",
    ]);
    let dbg = format!("{e:?}");
    assert!(!dbg.contains("hunter2-SECRET"), "{dbg}");
    assert!(!dbg.contains("tok-SECRET-123"), "{dbg}");
    assert!(dbg.contains("redacted"));
    assert!(dbg.contains("bob"));
}

#[test]
fn env_vars_supply_required_args() {
    let _g = no_env();
    std::env::set_var("SQLRUSTLER_HOST", "envhost");
    std::env::set_var("SQLRUSTLER_DATABASE", "envdb");
    let r = Cli::try_parse_from(["sqlrustler", "export"]);
    std::env::remove_var("SQLRUSTLER_HOST");
    std::env::remove_var("SQLRUSTLER_DATABASE");
    let Command::Export(e) = r.unwrap().command else {
        panic!()
    };
    assert_eq!(e.conn.host, "envhost");
    assert_eq!(e.conn.database, "envdb");
}

#[test]
fn bench_compare_only_sqlpackage() {
    let _g = no_env();
    assert!(parse(&["bench", "-H", "h", "-d", "db", "--compare", "sqlpackage"]).is_ok());
    assert!(parse(&["bench", "-H", "h", "-d", "db", "--compare", "bacpac"]).is_err());
}

#[test]
fn test_subcommand_only_values_and_helpers() {
    let _g = no_env();
    for (v, s, b, d) in [
        ("source", true, false, false),
        ("bandwidth", false, true, false),
        ("destination", false, false, true),
        ("all", true, true, true),
    ] {
        let Command::Test(t) = parse(&["test", "-H", "h", "-d", "db", "--only", v])
            .unwrap()
            .command
        else {
            panic!()
        };
        assert_eq!(
            (t.test_source(), t.test_bandwidth(), t.test_destination()),
            (s, b, d),
            "{v}"
        );
    }
    assert!(parse(&["test", "-H", "h", "-d", "db", "--only", "nope"]).is_err());
}

#[test]
fn pressure_subcommand_defaults() {
    let _g = no_env();
    let Command::Pressure(p) = parse(&["pressure", "-H", "h", "-d", "db"]).unwrap().command else {
        panic!()
    };
    assert_eq!(p.max_connections, 32);
    assert_eq!(p.step_secs, 10);
    assert!(!p.skip_local && !p.skip_network && !p.skip_db_ramp);
}

#[test]
fn tui_subcommand_needs_no_connection() {
    assert!(matches!(parse(&["tui"]).unwrap().command, Command::Tui));
}

#[test]
fn help_and_version_exit_cleanly() {
    let e = parse(&["--help"]).unwrap_err();
    assert_eq!(e.kind(), clap::error::ErrorKind::DisplayHelp);
    let e = parse(&["--version"]).unwrap_err();
    assert_eq!(e.kind(), clap::error::ErrorKind::DisplayVersion);
}

#[test]
fn unicode_and_spaces_in_args_accepted() {
    let _g = no_env();
    let e = export(&["-o", "dumpé 😀 file.rustler", "--exclude-tables", "tëst_*"]);
    assert_eq!(e.output.unwrap().to_str().unwrap(), "dumpé 😀 file.rustler");
    assert_eq!(e.exclude_tables, vec!["tëst_*"]);
}
