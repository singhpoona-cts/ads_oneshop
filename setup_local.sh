# Copyright 2026 Google LLC
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
#
# This script is used to set up environment variables for building and running
# components locally.

source env.sh
export GOOGLE_CLOUD_PROJECT="$(gcloud config get project)"
export PROJECT_NAME="${GOOGLE_CLOUD_PROJECT}"
export DATAFLOW_SERVICE_ACCOUNT="oneshop-dataflow-sa@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com"
export CLOUD_BUILD_SERVICE_ACCOUNT="$(terraform -chdir=infra/ output -json cloud_build_sa | jq -r)"
export DATAFLOW_REGION="$(terraform -chdir=infra/ output -json region | jq -r)"
export CLOUD_BUILD_LOGS_URL="$(terraform -chdir=infra/ output -json cloud_build_logs_url | jq -r)"
export IMAGES_REPO="$(terraform -chdir=infra/ output -json images_repo | jq -r)"
export STAGING_DIR="gs://${PROJECT_NAME}_oneshop_dataflow_staging/oneshop"
export GOOGLE_ADS_USE_PROTO_PLUS=False
export GOOGLE_ADS_CLIENT_ID="$(jq -r .web.client_id client_secrets.json)"
export GOOGLE_ADS_CLIENT_SECRET="$(jq -r .web.client_secret client_secrets.json)"
export GOOGLE_ADS_DEVELOPER_TOKEN="$(cat google_ads_developer_token.txt)"
export GOOGLE_ADS_REFRESH_TOKEN="$(cat refresh_token.txt)"
export USE_DATAFLOW_RUNNER=true
export DATAFLOW_TEMP_LOCATION="gs://${PROJECT_NAME}_oneshop_dataflow_temp/"
export DATAFLOW_REGION=us-central1
