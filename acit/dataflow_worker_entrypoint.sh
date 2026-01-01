#!/bin/bash
# Copyright 2025 Google LLC
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

# This script serves as the entrypoint for Dataflow workers.
# It executes a generator script to produce a shell script containing
# necessary environment variables and commands.
# The output of the generator is materialized to a temporary file
# because it captures dynamic environment details, such as the
# dynamically determined Python environment paths within the container,
# and sets special Beam-expected environment variables. Sourcing this
# materialized script ensures these variables are correctly set for the
# subsequent Beam boot process.
# This approach also allows for a significantly smaller Beam image,
# as only the bootstrap executable is required, with environment setup
# handled by this entrypoint.

set -e

boot_script_generator="$(rlocation ads_oneshop/acit/dataflow_worker_entrypoint_generator)"
"${boot_script_generator}" > /tmp/dataflow_boot_script
source /tmp/dataflow_boot_script

/opt/apache/beam/boot "$@"
