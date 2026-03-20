#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export DATAMART_BASE_PATH="${DATAMART_BASE_PATH:-$ROOT_DIR/data_lake/datamart}"

streamlit run "$ROOT_DIR/dashboard/app.py" --server.port 8501
