# SQLRustler — Setup Guide

Complete setup instructions from zero to running your first export.

---

## Prerequisites

### 1. Rust toolchain

SQLRustler requires Rust 1.80 or later.

**macOS / Linux:**
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
rustup --version   # verify
```

**Windows:**  
Download and run the installer from [rustup.rs](https://rustup.rs). Also install the
[MSVC Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) —
required for the TLS library.

**macOS — cmake (required by the TLS library):**
```bash
brew install cmake
```

**Linux — build tools:**
```bash
sudo apt install build-essential cmake pkg-config   # Debian/Ubuntu
sudo dnf install gcc cmake pkgconfig                # Fedora/RHEL
```

---

### 2. Azure CLI (optional — for Azure AD auth)

If you want to connect using your Azure AD account instead of a SQL username and password:

```bash
# macOS
brew install azure-cli

# Windows (PowerShell, as admin)
winget install Microsoft.AzureCLI

# Linux
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

Log in:
```bash
az login
```

Verify you can get a token for Azure SQL:
```bash
az account get-access-token \
  --resource https://database.windows.net/ \
  --query accessToken \
  --output tsv
```

If that prints a long token string, Azure AD auth will work in SQLRustler.

---

## Install SQLRustler

### Option A — Install from the repository (recommended)

```bash
git clone https://github.com/MrKruge/sqlrustler
cd sqlrustler
cargo install --path .
```

The binary is installed to `~/.cargo/bin/sqlrustler`.  
Make sure `~/.cargo/bin` is on your PATH (the Rust installer adds this automatically).

Verify:
```bash
sqlrustler --version
```

### Option B — Install from crates.io

```bash
cargo install sqlrustler
```

### Option C — Pre-built binary

Download from [GitHub Releases](https://github.com/MrKruge/sqlrustler/releases),
extract, and place the binary somewhere on your PATH.

| Platform | File |
|---|---|
| macOS Apple Silicon | `sqlrustler-vX.X.X-aarch64-apple-darwin.tar.gz` |
| macOS Intel | `sqlrustler-vX.X.X-x86_64-apple-darwin.tar.gz` |
| Linux x64 | `sqlrustler-vX.X.X-x86_64-unknown-linux-gnu.tar.gz` |
| Windows x64 | `sqlrustler-vX.X.X-x86_64-pc-windows-msvc.zip` |

---

## Verify the install

```bash
sqlrustler --version
sqlrustler --help
```

Run with no arguments to open the interactive TUI:

```bash
sqlrustler
```

---

## Connect to Azure SQL

SQLRustler supports two authentication methods. Use whichever your environment supports.

### Method 1 — Azure AD (recommended for Azure SQL)

No username or password needed. SQLRustler calls `az account get-access-token` at
run time using your active Azure CLI session.

```bash
az login                         # one-time login
sqlrustler                       # launch TUI → choose "Azure session" on the auth screen
```

Or via CLI flags:
```bash
export SQLRUSTLER_AAD_TOKEN=$(az account get-access-token \
  --resource https://database.windows.net/ \
  --query accessToken -o tsv)

sqlrustler export \
  -H myserver.database.windows.net \
  -d mydb \
  -o mydb.rustler
```

### Method 2 — SQL Server auth

```bash
sqlrustler export \
  -H myserver.database.windows.net \
  -d mydb \
  -u myuser \
  -p mypassword \
  -o mydb.rustler
```

> **Tip:** Use environment variables instead of `-p` to keep the password out of
> shell history and process listings:
> ```bash
> export SQLRUSTLER_HOST=myserver.database.windows.net
> export SQLRUSTLER_DATABASE=mydb
> export SQLRUSTLER_USER=myuser
> export SQLRUSTLER_PASSWORD=mypassword
>
> sqlrustler export -o mydb.rustler
> ```

---

## Azure SQL firewall

Your machine's public IP must be allowed through the Azure SQL server firewall.

**Azure Portal:**  
Open your SQL server → **Networking** → **Firewall rules** → add your IP.

**Azure CLI:**
```bash
MY_IP=$(curl -s https://api.ipify.org)

az sql server firewall-rule create \
  --resource-group <your-rg> \
  --server <your-server> \
  --name sqlrustler-dev \
  --start-ip-address $MY_IP \
  --end-ip-address $MY_IP
```

---

## Quick test — export a database

### Using the TUI

```bash
sqlrustler
```

1. Press any key at the welcome screen
2. Select **Export database to .rustler**
3. On the connection screen, press **[F1]** for Azure session or **[F2]** for manual
4. Enter your **host** and **database** name, press **Enter**
5. Adjust options (or just press **Enter** to accept defaults)
6. Review the confirm screen and press **Enter** to run

### Using the CLI directly

```bash
sqlrustler export \
  -H krugesql.database.windows.net \
  -d AdventureWorks \
  -o adventureworks.rustler
```

Expected output:
```
2026-10-08T12:00:00Z  INFO Extracting schema from AdventureWorks
2026-10-08T12:00:01Z  INFO Exporting 71 tables with parallelism=8
  Exporting dbo.SalesOrderHeader  ████████████ 31,465 rows  2.1s
  Exporting dbo.SalesOrderDetail  ████████████ 121,317 rows 4.8s
  ...
2026-10-08T12:00:18Z  INFO Export complete: 3,124,551 rows in 17.2s → adventureworks.rustler (142 MB)
```

---

## Inspect an archive

The `.rustler` format is plain tar + zstd. You can inspect it without SQLRustler:

```bash
# List contents
zstd -d mydb.rustler -o mydb.tar && tar -tf mydb.tar

# Read the manifest
tar -xOf mydb.tar manifest.json | python3 -m json.tool

# Read the schema
tar -xOf mydb.tar schema.sql | head -60
```

---

## Full import round-trip

```bash
# 1. Export the source database
sqlrustler export \
  -H source.database.windows.net \
  -d mydb \
  -o mydb.rustler

# 2. Import to a target database (must exist — or use --drop-existing to create it)
sqlrustler import \
  -H target.database.windows.net \
  -d mydb_restored \
  --drop-existing \
  -i mydb.rustler
```

---

## Benchmark vs sqlpackage

To compare against sqlpackage, install it first:  
[Download sqlpackage](https://learn.microsoft.com/en-us/sql/tools/sqlpackage/sqlpackage-download)

```bash
sqlrustler bench \
  -H myserver.database.windows.net \
  -d mydb \
  --compare sqlpackage
```

---

## Logging and debug output

Control log verbosity with `RUST_LOG`:

```bash
RUST_LOG=sqlrustler=debug sqlrustler export ...   # detailed per-table logs
RUST_LOG=sqlrustler=warn  sqlrustler export ...   # warnings and errors only
RUST_LOG=off              sqlrustler export ...   # silent
```

---

## Troubleshooting

**`command not found: sqlrustler`**  
Rust installs binaries to `~/.cargo/bin`. Make sure it's on your PATH:
```bash
export PATH="$HOME/.cargo/bin:$PATH"
echo 'export PATH="$HOME/.cargo/bin:$PATH"' >> ~/.zshrc   # or ~/.bashrc
```

**`cargo: command not found`**  
Run the Rust installer: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

**`cmake not found` during build (macOS)**  
```bash
brew install cmake
```

**Connection refused / timeout**  
Check the Azure SQL firewall — your IP needs to be whitelisted (see Azure SQL firewall section above).

**`az account get-access-token` fails**  
```bash
az login          # re-authenticate
az account show   # verify the right tenant is active
```

**`Failed to enable IDENTITY_INSERT`**  
Your SQL login needs `db_owner` or `ALTER TABLE` permission on the target database.

**Large databases run out of memory on import**  
This is a known v0.1 limitation — the archive is fully loaded into RAM before import starts.
Streaming import is planned for v1.1. Workaround: split the export by schema or use a
machine with more RAM.

---

## Uninstall

```bash
cargo uninstall sqlrustler
```

---

## Getting help

- Issues: [github.com/MrKruge/sqlrustler/issues](https://github.com/MrKruge/sqlrustler/issues)
- Run `sqlrustler --help` or `sqlrustler export --help` for flag reference
