#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_NAME="${1:-}"
TEMPLATE_PATH="$ROOT_DIR/config/companies/first-customer.sample.env"

if [[ -z "$PROFILE_NAME" ]]; then
  echo "usage: bash scripts/create_company_profile.sh <company-profile-name>" >&2
  exit 1
fi

if [[ ! "$PROFILE_NAME" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
  echo "invalid profile name: use lowercase letters, numbers, and hyphen" >&2
  exit 1
fi

TARGET_PATH="$ROOT_DIR/config/companies/${PROFILE_NAME}.env"

if [[ -f "$TARGET_PATH" ]]; then
  echo "profile already exists: $TARGET_PATH" >&2
  exit 1
fi

cp "$TEMPLATE_PATH" "$TARGET_PATH"
echo "created profile: $TARGET_PATH"
echo "next: edit this file with company-specific interfaces and expected IDs"
