#!/usr/bin/env bash

set -euo pipefail

load_company_config() {
  local root_dir="$1"
  local profile="${ROOTSYS_COMPANY_PROFILE:-default}"
  local default_config="$root_dir/config/companies/${profile}.env"
  local config_file="${ROOTSYS_CONFIG_FILE:-$default_config}"
  local configurable_keys=(
    ROOTSYS_CONTRACT_REGISTRY
    ROOTSYS_INTERFACE_MES
    ROOTSYS_INTERFACE_QMS
    ROOTSYS_INTERFACE_STREAM
    ROOTSYS_INTERFACE_REST_SMOKE
    ROOTSYS_INTERFACE_POSTGRES_SMOKE
    ROOTSYS_INTERFACE_MYSQL_SMOKE
    ROOTSYS_MES_ROW_COUNT
    ROOTSYS_QMS_ROW_COUNT
    ROOTSYS_SMOKE_DB_COUNT
    ROOTSYS_SMOKE_REST_COUNT
    ROOTSYS_SMOKE_REST_ID_PREFIX
    ROOTSYS_COMPLEX_STREAM_RECORD_COUNT
    ROOTSYS_COMPLEX_INTERVAL_RUNS
    ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT
    ROOTSYS_COMPLEX_REPLAY_INTERFACE_NAME
    ROOTSYS_COMPLEX_REPLAY_INTERFACE_VERSION
    ROOTSYS_UI_DIR
  )
  local preloaded_keys=()
  local preloaded_values=()

  for key in "${configurable_keys[@]}"; do
    if [[ "${!key+x}" == "x" ]]; then
      preloaded_keys+=("$key")
      preloaded_values+=("${!key}")
    fi
  done

  if [[ -f "$config_file" ]]; then
    set -a
    source "$config_file"
    set +a
    export ROOTSYS_ACTIVE_CONFIG_FILE="$config_file"
  else
    export ROOTSYS_ACTIVE_CONFIG_FILE=""
  fi

  local idx
  for idx in "${!preloaded_keys[@]}"; do
    export "${preloaded_keys[$idx]}=${preloaded_values[$idx]}"
  done

  export ROOTSYS_CONTRACT_REGISTRY="${ROOTSYS_CONTRACT_REGISTRY:-$root_dir/system/contracts/reference/allowlist.json}"

  export ROOTSYS_INTERFACE_MES="${ROOTSYS_INTERFACE_MES:-$root_dir/tests/fixtures/interfaces/mes.db.json}"
  export ROOTSYS_INTERFACE_QMS="${ROOTSYS_INTERFACE_QMS:-$root_dir/tests/fixtures/interfaces/qms.db.json}"
  export ROOTSYS_INTERFACE_STREAM="${ROOTSYS_INTERFACE_STREAM:-$root_dir/tests/fixtures/interfaces/stream.kafka.sample.json}"

  export ROOTSYS_INTERFACE_REST_SMOKE="${ROOTSYS_INTERFACE_REST_SMOKE:-$root_dir/tests/fixtures/interfaces/rest.smoke.json}"
  export ROOTSYS_INTERFACE_POSTGRES_SMOKE="${ROOTSYS_INTERFACE_POSTGRES_SMOKE:-$root_dir/tests/fixtures/interfaces/postgres.smoke.json}"
  export ROOTSYS_INTERFACE_MYSQL_SMOKE="${ROOTSYS_INTERFACE_MYSQL_SMOKE:-$root_dir/tests/fixtures/interfaces/mysql.smoke.json}"

  export ROOTSYS_MES_ROW_COUNT="${ROOTSYS_MES_ROW_COUNT:-200}"
  export ROOTSYS_QMS_ROW_COUNT="${ROOTSYS_QMS_ROW_COUNT:-200}"
  export ROOTSYS_SMOKE_DB_COUNT="${ROOTSYS_SMOKE_DB_COUNT:-200}"
  export ROOTSYS_SMOKE_REST_COUNT="${ROOTSYS_SMOKE_REST_COUNT:-200}"
  export ROOTSYS_SMOKE_REST_ID_PREFIX="${ROOTSYS_SMOKE_REST_ID_PREFIX:-rest}"
  export ROOTSYS_COMPLEX_STREAM_RECORD_COUNT="${ROOTSYS_COMPLEX_STREAM_RECORD_COUNT:-300}"
  export ROOTSYS_COMPLEX_INTERVAL_RUNS="${ROOTSYS_COMPLEX_INTERVAL_RUNS:-2}"
  export ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT="${ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT:-50}"

  export ROOTSYS_COMPLEX_REPLAY_INTERFACE_NAME="${ROOTSYS_COMPLEX_REPLAY_INTERFACE_NAME:-mes}"
  export ROOTSYS_COMPLEX_REPLAY_INTERFACE_VERSION="${ROOTSYS_COMPLEX_REPLAY_INTERFACE_VERSION:-v1}"
  export ROOTSYS_CONTRACT_REGISTRY="$(resolve_path "$root_dir" "$ROOTSYS_CONTRACT_REGISTRY")"
  export ROOTSYS_INTERFACE_MES="$(resolve_path "$root_dir" "$ROOTSYS_INTERFACE_MES")"
  export ROOTSYS_INTERFACE_QMS="$(resolve_path "$root_dir" "$ROOTSYS_INTERFACE_QMS")"
  export ROOTSYS_INTERFACE_STREAM="$(resolve_path "$root_dir" "$ROOTSYS_INTERFACE_STREAM")"
  export ROOTSYS_INTERFACE_REST_SMOKE="$(resolve_path "$root_dir" "$ROOTSYS_INTERFACE_REST_SMOKE")"
  export ROOTSYS_INTERFACE_POSTGRES_SMOKE="$(resolve_path "$root_dir" "$ROOTSYS_INTERFACE_POSTGRES_SMOKE")"
  export ROOTSYS_INTERFACE_MYSQL_SMOKE="$(resolve_path "$root_dir" "$ROOTSYS_INTERFACE_MYSQL_SMOKE")"
  export ROOTSYS_UI_DIR="$(resolve_path "$root_dir" "${ROOTSYS_UI_DIR:-$root_dir/ui}")"
}

validate_company_config() {
  local missing=0
  local required_files=(
    "$ROOTSYS_CONTRACT_REGISTRY"
    "$ROOTSYS_INTERFACE_MES"
    "$ROOTSYS_INTERFACE_QMS"
    "$ROOTSYS_INTERFACE_STREAM"
    "$ROOTSYS_INTERFACE_REST_SMOKE"
    "$ROOTSYS_INTERFACE_POSTGRES_SMOKE"
    "$ROOTSYS_INTERFACE_MYSQL_SMOKE"
  )

  for file in "${required_files[@]}"; do
    if [[ ! -f "$file" ]]; then
      echo "required config file not found: $file" >&2
      missing=1
    fi
  done

  local numeric_keys=(
    ROOTSYS_MES_ROW_COUNT
    ROOTSYS_QMS_ROW_COUNT
    ROOTSYS_SMOKE_DB_COUNT
    ROOTSYS_SMOKE_REST_COUNT
    ROOTSYS_COMPLEX_STREAM_RECORD_COUNT
    ROOTSYS_COMPLEX_INTERVAL_RUNS
    ROOTSYS_COMPLEX_REPLAY_INPUT_COUNT
  )

  for key in "${numeric_keys[@]}"; do
    local value="${!key}"
    if [[ ! "$value" =~ ^[0-9]+$ ]] || [[ "$value" -le 0 ]]; then
      echo "invalid numeric config: $key=$value (must be positive integer)" >&2
      missing=1
    fi
  done

  if [[ "$missing" -ne 0 ]]; then
    return 1
  fi
}

resolve_path() {
  local root_dir="$1"
  local input="$2"

  if [[ "$input" = /* ]]; then
    printf "%s" "$input"
  else
    printf "%s" "$root_dir/$input"
  fi
}
