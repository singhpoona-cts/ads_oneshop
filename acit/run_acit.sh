#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# shellcheck source=bq.sh
source "$(rlocation ads_oneshop/acit/bq.sh)"

if [[ -z "$CUSTOMER_IDS" ]]; then
    echo '$CUSTOMER_IDS is required. Comma-delimited.' 1>&2
    exit 1
fi

if [[ -z "$MERCHANT_IDS" ]]; then
    echo '$MERCHANT_IDS is required. Comma-delimited.' 1>&2
    exit 1
fi

if [[ -z "$STAGING_DIR" ]]; then
    echo '$STAGING_DIR is required' 1>&2
    exit 1
fi

if [[ -z "$PROJECT_NAME" ]]; then
    echo '$PROJECT_NAME is required' 1>&2
    exit 1
fi

if [[ -z "$DATASET_NAME" ]]; then
    echo '$DATASET_NAME is required' 1>&2
    exit 1
fi

if [[ -z "$DATASET_LOCATION" ]]; then
    echo '$DATASET_LOCATION (e.g. "US") is required' 1>&2
    exit 1
fi

if [[ -z "${ADMIN}" ]]; then
  echo '$ADMIN is not defined. Please declare as true or false.' 1>&2
  exit 1
fi

# Experimental flag for using the Dataflow runner
USE_DATAFLOW_RUNNER="${USE_DATAFLOW_RUNNER:-false}"

if [[ "${USE_DATAFLOW_RUNNER}" = 'true' ]]; then
  if [[ -z "${DATAFLOW_TEMP_LOCATION}" ]]; then
    echo '$DATAFLOW_TEMP_LOCATION is required when using the dataflow runner.' 1>&2
    exit 1
  fi
  if [[ -z "${DATAFLOW_REGION}" ]]; then
    echo '$DATAFLOW_REGION is required when using the dataflow runner.' 1>&2
    exit 1
  fi
  if [[ -z "${DATAFLOW_SERVICE_ACCOUNT}" ]]; then
    echo '$DATAFLOW_SERVICE_ACCOUNT is required when using the dataflow runner.' 1>&2
    exit 1
  fi
  if [[ -z "${IMAGES_REPO}" ]]; then
    echo '$IMAGES_REPO is required when using the dataflow runner.' 1>&2
    exit 1
  fi
fi

USE_TEST_ACCOUNTS="${USE_TEST_ACCOUNTS:-false}"

DEFAULT_RUN_ID="$(date -Iseconds)"

# TODO: Add optional flag for skipping download

RUN_ID="${RUN_ID:-${DEFAULT_RUN_ID}}"

SOURCES_DIR="${STAGING_DIR}/${RUN_ID}/sources"
SINKS_DIR="${STAGING_DIR}/${RUN_ID}/sinks"
BQ_DIR="${STAGING_DIR}/${RUN_ID}/bq"

declare -a merchant_id_flags
IFS=',' read -ra merchant_ids <<< "$MERCHANT_IDS"
for merchant_id in "${merchant_ids[@]}"; do
  merchant_id_flags+=(--merchant_id="${merchant_id}")
done

declare -a customer_id_flags
IFS=',' read -ra customer_ids <<< "$CUSTOMER_IDS"
for customer_id in "${customer_ids[@]}"; do
  customer_id_flags+=(--customer_id="${customer_id}")
done

pull_data() {
  echo "Pulling data"
  "$(rlocation ads_oneshop/acit/acit)" \
    "${customer_id_flags[@]}" \
    "${merchant_id_flags[@]}" \
    --admin="${ADMIN}" \
    --output="${SOURCES_DIR}" \
    --use_test_accounts="${USE_TEST_ACCOUNTS}"
  echo "Data saved to ${SOURCES_DIR}"
}

run_pipeline() {
  echo "Running product pipeline"
  if [[ "${USE_DATAFLOW_RUNNER}" = 'true' ]]; then
    "$(rlocation ads_oneshop/acit/create_base_tables)" \
      --products_output="${SINKS_DIR}/wide_products_table.jsonlines" \
      --liasettings_output="${SINKS_DIR}/liasettings.jsonlines" \
      --source_dir="${SOURCES_DIR}" \
      -- \
      --service_account_email="${DATAFLOW_SERVICE_ACCOUNT}" \
      --region "${DATAFLOW_REGION}" \
      --runner DataflowRunner \
      --project "${PROJECT_NAME}" \
      --temp_location "${DATAFLOW_TEMP_LOCATION}" \
      --sdk_container_image="${DATAFLOW_REGION}-docker.pkg.dev/${PROJECT_NAME}/${IMAGES_REPO}/dataflow:latest" \
      --sdk_location=container
  else
    rm -rf "${SINKS_DIR}" && mkdir -p "${SINKS_DIR}"
    "$(rlocation ads_oneshop/acit/create_base_tables)" \
      --products_output="${SINKS_DIR}/wide_products_table.jsonlines" \
      --liasettings_output="${SINKS_DIR}/liasettings.jsonlines" \
      --source_dir="${SOURCES_DIR}" \
      -- \
      --runner=direct
  fi
  echo "Product data saved to ${SINKS_DIR}"
}

upload_to_bq() {
  local -i ttl="$(( 60 * 60 * 24 * 60 ))"

  # bq cli can load from wildcards, with limitations
  if [[ "${USE_DATAFLOW_RUNNER}" = 'true' ]]; then
    # BQ_DIR isn't used at all in this case
    local accounts_path="${SOURCES_DIR}/merchant_center/*/accounts/rows.jsonlines"
    if [[ "${ADMIN}" = true ]]; then
      local shippingsettings_path="${SOURCES_DIR}/merchant_center/*/shippingsettings/rows.jsonlines"
      local liasettings_path="${SINKS_DIR}/liasettings.jsonlines-*"
    fi
    local performance_path="${SOURCES_DIR}/ads/all/shopping_performance_view/*rows.jsonlines"
    local language_path="${SOURCES_DIR}/ads/all/language_constant/*rows.jsonlines"
    local products_path="${SINKS_DIR}/wide_products_table.jsonlines-*"
  else
    rm -rf "${BQ_DIR}" && mkdir -p "${BQ_DIR}"
    local accounts_path="${BQ_DIR}/accounts.jsonlines"
    cat $(find "${SOURCES_DIR}" -type f | grep accounts) > "${accounts_path}"
    if [[ "${ADMIN}" = true ]]; then
      local shippingsettings_path="${BQ_DIR}/shippingsettings.jsonlines"
      cat $(find "${SOURCES_DIR}" -type f | grep shippingsettings) > "${shippingsettings_path}"
      local liasettings_path="${BQ_DIR}/liasettings.jsonlines"
      cat $(find "${SINKS_DIR}" -type f | grep liasettings) > "${liasettings_path}"
    fi
    local performance_path="${BQ_DIR}/performance.jsonlines"
    cat $(find "${SOURCES_DIR}" -type f | grep performance) > "${performance_path}"
    local language_path="${BQ_DIR}/language.jsonlines"
    cat $(find "${SOURCES_DIR}" -type f | grep language) > "${language_path}"
    local products_path="${BQ_DIR}/products.jsonlines"
    cat $(find "${SINKS_DIR}" -type f | grep wide_products_table ) > "${products_path}"
  fi

  # Create the dataset if it doesn't exist yet
  bq::create_dataset

  bq::load \
    "${accounts_path}" \
    "accounts" \
    "NEWLINE_DELIMITED_JSON" \
    "${ttl}" \
    "$(rlocation ads_oneshop/acit/schemas/acit/accounts.schema)"

  if [[ "${ADMIN}" = true ]]; then
    bq::load \
      "${shippingsettings_path}" \
      "shippingsettings" \
      "NEWLINE_DELIMITED_JSON" \
      "${ttl}" \
      "$(rlocation ads_oneshop/acit/schemas/acit/shippingsettings.schema)"

    bq::load \
      "${liasettings_path}" \
      "liasettings" \
      "NEWLINE_DELIMITED_JSON" \
      "${ttl}" \
      "$(rlocation ads_oneshop/acit/api/v0/storage/liasettings.schema)"
  fi

  bq::load \
    "${performance_path}" \
    "performance" \
    "NEWLINE_DELIMITED_JSON" \
    "${ttl}" \
    "$(rlocation ads_oneshop/acit/schemas/acit/performance.schema)"

  bq::load \
    "${language_path}" \
    "language" \
    "NEWLINE_DELIMITED_JSON" \
    "${ttl}"

  bq::load \
    "${products_path}" \
    "products" \
    "NEWLINE_DELIMITED_JSON" \
    "${ttl}" \
    "$(rlocation ads_oneshop/acit/api/v0/storage/Products.schema)"
}

create_views() {
  bq::run_ddl "" < <(envsubst < "$(rlocation ads_oneshop/acit/views/main_view.sql)")
  bq::run_ddl "" < <(envsubst < "$(rlocation ads_oneshop/acit/views/disapprovals_view.sql)")
}

run_extensions() {
  RUN_MERCHANT_EXCELLENCE="${RUN_MERCHANT_EXCELLENCE:-false}"
  if [[ "${RUN_MERCHANT_EXCELLENCE}" = 'true' ]]; then
    "$(rlocation "ads_oneshop/extensions/merchant_excellence/run_mex")"
  fi
}

pull_data
run_pipeline
upload_to_bq
create_views
run_extensions
