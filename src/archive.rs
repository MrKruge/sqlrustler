use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

// ── Manifest types ────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Manifest {
    pub sqlrustler_version: String,
    pub format_version: u32,
    pub created_at: String,
    pub source_host: String,
    pub source_database: String,
    pub tables: Vec<ManifestTable>,
    pub total_rows: u64,
    pub schema_only: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManifestTable {
    pub table_fqn: String,
    /// Relative path inside archive: "data/dbo_Orders.parquet"
    pub file_name: String,
    pub row_count: u64,
    pub has_identity: bool,
    pub identity_columns: Vec<String>,
}

// ── Archive writer ────────────────────────────────────────────────────────────

pub struct ArchiveWriter {
    builder: tar::Builder<zstd::Encoder<'static, std::fs::File>>,
}

impl ArchiveWriter {
    pub fn create(path: &Path, compression_level: i32) -> Result<Self> {
        let file = std::fs::File::create(path)
            .with_context(|| format!("Failed to create archive: {}", path.display()))?;
        let encoder = zstd::Encoder::new(file, compression_level)
            .context("Failed to create zstd encoder")?;
        let builder = tar::Builder::new(encoder);
        Ok(Self { builder })
    }

    pub fn write_schema_sql(&mut self, sql: &str) -> Result<()> {
        let bytes = sql.as_bytes();
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        self.builder
            .append_data(&mut header, "schema.sql", bytes)
            .context("Failed to write schema.sql to archive")?;
        Ok(())
    }

    pub fn write_manifest(&mut self, manifest: &Manifest) -> Result<()> {
        let json = serde_json::to_string_pretty(manifest)
            .context("Failed to serialize manifest")?;
        let bytes = json.as_bytes();
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        self.builder
            .append_data(&mut header, "manifest.json", bytes)
            .context("Failed to write manifest.json to archive")?;
        Ok(())
    }

    pub fn add_parquet_file(&mut self, archive_name: &str, path: &Path) -> Result<()> {
        let mut file = std::fs::File::open(path)
            .with_context(|| format!("Failed to open parquet file: {}", path.display()))?;
        let file_size = file.metadata()?.len();

        let mut header = tar::Header::new_gnu();
        header.set_size(file_size);
        header.set_mode(0o644);
        header.set_cksum();

        self.builder
            .append_data(&mut header, archive_name, &mut file)
            .context("Failed to add parquet file to archive")?;
        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        let encoder = self.builder.into_inner().context("Failed to finalize tar")?;
        encoder.finish().context("Failed to finalize zstd encoder")?;
        Ok(())
    }
}

// ── Archive reader ────────────────────────────────────────────────────────────
// P4: For v1 we read all entries into a HashMap. Large DBs (>50 GB) will need
// streaming access — TODO(streaming-archive-read): use seekable frames + index.

pub struct ArchiveReader {
    entries: HashMap<String, Vec<u8>>,
}

impl ArchiveReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("Failed to open archive: {}", path.display()))?;
        let decoder = zstd::Decoder::new(file).context("Failed to create zstd decoder")?;
        let mut archive = tar::Archive::new(decoder);

        let mut entries: HashMap<String, Vec<u8>> = HashMap::new();
        for entry in archive.entries().context("Failed to read archive entries")? {
            let mut entry = entry.context("Failed to read archive entry")?;
            let path_str = entry
                .path()
                .context("Failed to get entry path")?
                .to_str()
                .unwrap_or("")
                .to_string();
            let mut data = Vec::new();
            entry
                .read_to_end(&mut data)
                .context("Failed to read entry data")?;
            entries.insert(path_str, data);
        }

        Ok(Self { entries })
    }

    pub fn read_manifest(&self) -> Result<Manifest> {
        let bytes = self
            .entries
            .get("manifest.json")
            .ok_or_else(|| anyhow!("manifest.json not found in archive"))?;
        serde_json::from_slice(bytes).context("Failed to parse manifest.json")
    }

    pub fn read_schema_sql(&self) -> Result<String> {
        let bytes = self
            .entries
            .get("schema.sql")
            .ok_or_else(|| anyhow!("schema.sql not found in archive"))?;
        String::from_utf8(bytes.clone()).context("schema.sql is not valid UTF-8")
    }

    pub fn read_parquet(&self, file_name: &str) -> Result<Vec<u8>> {
        self.entries
            .get(file_name)
            .cloned()
            .ok_or_else(|| anyhow!("Parquet file not found in archive: {file_name}"))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Sanitize a table FQN like "[dbo].[Orders]" to a safe filename component.
pub fn fqn_to_filename(fqn: &str) -> String {
    fqn.replace(['[', ']'], "")
        .replace('.', "_")
        .trim_matches('_')
        .to_string()
}

/// Return the archive-internal path for a table's Parquet file.
pub fn parquet_archive_path(fqn: &str) -> String {
    format!("data/{}.parquet", fqn_to_filename(fqn))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // ── fqn_to_filename ───────────────────────────────────────────────────────

    #[test]
    fn fqn_to_filename_simple() {
        assert_eq!(fqn_to_filename("[dbo].[Orders]"), "dbo_Orders");
    }

    #[test]
    fn fqn_to_filename_with_spaces() {
        assert_eq!(fqn_to_filename("[my schema].[My Table]"), "my schema_My Table");
    }

    #[test]
    fn fqn_to_filename_strips_leading_trailing_underscores() {
        // The function replaces '.' with '_' then trims '_' at edges.
        // "[dbo].[T]" → "dbo_T" (no edge underscores)
        assert_eq!(fqn_to_filename("[dbo].[T]"), "dbo_T");
    }

    // ── parquet_archive_path ──────────────────────────────────────────────────

    #[test]
    fn parquet_archive_path_simple() {
        assert_eq!(parquet_archive_path("[dbo].[Orders]"), "data/dbo_Orders.parquet");
    }

    #[test]
    fn parquet_archive_path_with_spaces() {
        assert_eq!(
            parquet_archive_path("[my schema].[My Table]"),
            "data/my schema_My Table.parquet"
        );
    }

    // ── ArchiveWriter + ArchiveReader round-trip ──────────────────────────────

    fn sample_manifest() -> Manifest {
        Manifest {
            sqlrustler_version: "0.1.0-test".to_string(),
            format_version: 1,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            source_host: "testhost".to_string(),
            source_database: "testdb".to_string(),
            tables: vec![ManifestTable {
                table_fqn: "[dbo].[Orders]".to_string(),
                file_name: "data/dbo_Orders.parquet".to_string(),
                row_count: 42,
                has_identity: true,
                identity_columns: vec!["Id".to_string()],
            }],
            total_rows: 42,
            schema_only: false,
        }
    }

    #[test]
    fn archive_round_trip_manifest_and_schema() {
        let dir = tempdir().unwrap();
        let archive_path = dir.path().join("test.rustler");

        let manifest = sample_manifest();
        let schema_sql = "CREATE TABLE [dbo].[Orders] (Id INT IDENTITY(1,1) NOT NULL);";
        // Arbitrary synthetic "parquet" bytes — ArchiveReader stores them as Vec<u8>
        let fake_parquet: Vec<u8> = vec![0x50, 0x41, 0x52, 0x31, 0xDE, 0xAD, 0xBE, 0xEF];

        // Write parquet bytes to a temp file so add_parquet_file can open it
        let parquet_tmp = dir.path().join("dbo_Orders.parquet");
        std::fs::write(&parquet_tmp, &fake_parquet).unwrap();

        // Write archive
        {
            let mut writer = ArchiveWriter::create(&archive_path, 1).unwrap();
            writer.write_manifest(&manifest).unwrap();
            writer.write_schema_sql(schema_sql).unwrap();
            writer
                .add_parquet_file("data/dbo_Orders.parquet", &parquet_tmp)
                .unwrap();
            writer.finish().unwrap();
        }

        // Read archive back
        let reader = ArchiveReader::open(&archive_path).unwrap();

        // Verify manifest
        let read_manifest = reader.read_manifest().unwrap();
        assert_eq!(read_manifest.sqlrustler_version, manifest.sqlrustler_version);
        assert_eq!(read_manifest.format_version, manifest.format_version);
        assert_eq!(read_manifest.source_database, manifest.source_database);
        assert_eq!(read_manifest.total_rows, manifest.total_rows);
        assert_eq!(read_manifest.tables.len(), 1);
        assert_eq!(read_manifest.tables[0].table_fqn, "[dbo].[Orders]");
        assert_eq!(read_manifest.tables[0].row_count, 42);
        assert!(read_manifest.tables[0].has_identity);
        assert_eq!(read_manifest.tables[0].identity_columns, vec!["Id"]);

        // Verify schema.sql
        let read_schema = reader.read_schema_sql().unwrap();
        assert_eq!(read_schema, schema_sql);

        // Verify Parquet bytes (byte-for-byte)
        let read_parquet = reader.read_parquet("data/dbo_Orders.parquet").unwrap();
        assert_eq!(read_parquet, fake_parquet);
    }

    #[test]
    fn archive_reader_missing_manifest_returns_error() {
        let dir = tempdir().unwrap();
        let archive_path = dir.path().join("empty.rustler");

        // Write archive with schema only, no manifest
        {
            let mut writer = ArchiveWriter::create(&archive_path, 1).unwrap();
            writer.write_schema_sql("-- no tables").unwrap();
            writer.finish().unwrap();
        }

        let reader = ArchiveReader::open(&archive_path).unwrap();
        assert!(reader.read_manifest().is_err(), "should error when manifest.json missing");
    }

    #[test]
    fn archive_reader_missing_parquet_returns_error() {
        let dir = tempdir().unwrap();
        let archive_path = dir.path().join("no_parquet.rustler");

        {
            let mut writer = ArchiveWriter::create(&archive_path, 1).unwrap();
            writer.write_manifest(&sample_manifest()).unwrap();
            writer.write_schema_sql("-- schema").unwrap();
            writer.finish().unwrap();
        }

        let reader = ArchiveReader::open(&archive_path).unwrap();
        assert!(
            reader.read_parquet("data/nonexistent.parquet").is_err(),
            "should error when parquet file missing"
        );
    }
}
