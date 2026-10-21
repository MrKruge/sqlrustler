# sqlrustler — Build Team Agent Pack

This directory contains agent skills for the **sqlrustler build team** — the
engineers building the tool, not end users running it.

Each skill primes your AI agent with a specific engineering persona.
Load the one (or ones) relevant to the task at hand.

## Available agents

| Skill | When to use |
|---|---|
| `rust-engineer` | Rust idioms, ownership, async correctness, macro hygiene, API surface |
| `database-architect` | SQL type mapping, DDL generation, FK ordering, data round-trip fidelity |
| `security-auditor` | Credential handling, SQL injection surface, TLS, secret leakage |
| `performance-engineer` | Throughput, parallelism, memory model, INSERT vs bulk_insert |
| `qa-engineer` | Test coverage gaps, integration test matrix, release checklist |
| `devops-engineer` | CI/CD, cross-compilation, crates.io publish, binary releases |

## Quickstart

```bash
# Load the build-team context into Claude Code (run from repo root)
bash build-agents/adapters/install.sh
```

Or load a single skill manually by pointing your agent at the relevant `SKILL.md`.

## Project context

See `build-agents/adapters/CLAUDE.md` for the shared project layout, archive
format, and core rules every skill should respect.

Never ship the `build-agents/` directory as part of the published crate —
`Cargo.toml`'s `exclude` field should list it.
