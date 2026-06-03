#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_FILE="$REPO_ROOT/scripts/certificates/certificates.txt"
VENV_PY="$REPO_ROOT/.venv/bin/python"

set -a
source "$REPO_ROOT/.env"
set +a

: "${KV_NAMESPACE_ID:?KV_NAMESPACE_ID not set in .env}"

for cmd in wrangler jq "$VENV_PY"; do
  command -v "$cmd" >/dev/null 2>&1 || { [ -x "$cmd" ] || { echo "Missing: $cmd" >&2; exit 1; }; }
done

echo "==> Exporting certificate URLs from KV namespace $KV_NAMESPACE_ID"
wrangler kv key list --namespace-id "$KV_NAMESPACE_ID" --remote \
  | jq -r '.[] | .metadata.certificateUrl' \
  > "$CERT_FILE"

echo "==> $(wc -l < "$CERT_FILE" | tr -d ' ') certificate URLs written to $CERT_FILE"

echo "==> Running pipeline"
cd "$REPO_ROOT/scripts"
"$VENV_PY" main.py "$@"
