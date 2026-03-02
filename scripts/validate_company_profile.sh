#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -ge 1 ]]; then
  export ROOTSYS_COMPANY_PROFILE="$1"
fi

source "$ROOT_DIR/scripts/lib/company_config.sh"
load_company_config "$ROOT_DIR"
validate_company_config

echo "profile validation passed"
if [[ -n "$ROOTSYS_ACTIVE_CONFIG_FILE" ]]; then
  echo "active config: $ROOTSYS_ACTIVE_CONFIG_FILE"
else
  echo "active config: defaults"
fi

echo "resolved interfaces:"
echo "- mes: $ROOTSYS_INTERFACE_MES"
echo "- qms: $ROOTSYS_INTERFACE_QMS"
echo "- stream: $ROOTSYS_INTERFACE_STREAM"
echo "- rest-smoke: $ROOTSYS_INTERFACE_REST_SMOKE"
echo "- postgres-smoke: $ROOTSYS_INTERFACE_POSTGRES_SMOKE"
echo "- mysql-smoke: $ROOTSYS_INTERFACE_MYSQL_SMOKE"
