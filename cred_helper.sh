#!/bin/bash
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
# cred_helper.sh: A credential helper for accessing private artifact registry pip packages.
if [[ "${1}" != "get" ]]; then
    echo "Must call via 'cred_helper get'".
    exit 1
fi

# Note: formatting is optional
echo '{'
echo '  "headers": {'
echo '    "Authorization": ["Bearer '$(gcloud auth print-access-token)'"]'
echo '  }'
echo '}'
