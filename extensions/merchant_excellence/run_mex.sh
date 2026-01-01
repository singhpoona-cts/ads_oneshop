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

# shellcheck source=../../acit/bq.sh
source "$(rlocation ads_oneshop/acit/bq.sh)"

create_views() {
  # Define parameters for running `bq` commands
  CURRENT_DATE="$(date '+%Y%m%d')"

  # Loading the Benchmark files into BQ Tables
  bq::load \
    "$(rlocation ads_oneshop/benchmark/benchmark_values.csv)" \
    "MEX_benchmark_values" \
    "CSV" \
    "${TTL}" \
    ""
  bq::load \
    "$(rlocation ads_oneshop/benchmark/benchmark_details.csv)" \
    "MEX_benchmark_details" \
    "CSV" \
    "${TTL}" \
    ""

  # Running Merchant Excellence queries
  bq::run_ddl "" < <(envsubst < "$(rlocation ads_oneshop/extensions/merchant_excellence/admin_tables.sql)")
  bq::run_ddl "" < <(envsubst < "$(rlocation ads_oneshop/extensions/merchant_excellence/offer_list.sql)")
  bq::run_ddl "" < <(envsubst < "$(rlocation ads_oneshop/extensions/merchant_excellence/account_list.sql)")
  bq::run_ddl "" < <(envsubst < "$(rlocation ads_oneshop/extensions/merchant_excellence/benchmark_scores.sql)")
  bq::run_ddl "" < <(envsubst < "$(rlocation ads_oneshop/extensions/merchant_excellence/all_metrics.sql)")
  bq::run_ddl "MEX_All_Metrics_historical\$${CURRENT_DATE}" < <(envsubst < "$(rlocation ads_oneshop/extensions/merchant_excellence/all_metrics_historical.sql)")
  bq::run_ddl "" < <(envsubst < "$(rlocation ads_oneshop/extensions/merchant_excellence/offer_funnel.sql)")
  bq::run_ddl "MEX_Offer_Funnel_historical\$${CURRENT_DATE}" < <(envsubst < "$(rlocation ads_oneshop/extensions/merchant_excellence/offer_funnel_historical.sql)")
  bq::run_ddl < <(envsubst < "$(rlocation ads_oneshop/extensions/merchant_excellence/ml_data.sql)")
}

create_views
