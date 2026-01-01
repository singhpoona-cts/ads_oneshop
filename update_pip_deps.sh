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
# Updates dependencies when pinned to mirrored versions in Artifact registry.
# Do not run if not using Artifact Registry.
# copybara:strip_begin

# First, install uv via pipx. Then, create a virtual environment using the following command
#   virtualenv --python=/usr/bin/python3.12 .venv
# Then, run the following:

uv pip compile --index-url https://oauth2accesstoken:$(gcloud auth print-access-token)@us-python.pkg.dev/artifact-foundry-prod/python-3p-trusted/simple --no-strip-extras --generate-hashes requirements.in -o requirements_lock.txt

# Register updated requirements with bazel
# TODO: figure out why this conflicts
# bazel run //:requirements.update

# Sanity checks
# bazel build //...
# TODO(cbartz): disable //:requirements_test because UV conflicts
# bazel test //...
# copybara:strip_end
