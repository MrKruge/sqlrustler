#!/usr/bin/env bash
# sqlrustler — Build team agent adapter installer
# Usage: bash build-agents/adapters/install.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

mkdir -p "$REPO_ROOT/.claude"
cp "$SCRIPT_DIR/CLAUDE.md" "$REPO_ROOT/.claude/CLAUDE.md"
echo "  [claude]   installed -> .claude/CLAUDE.md"

echo "Done. Load a skill by telling your agent to read the relevant SKILL.md in build-agents/."
