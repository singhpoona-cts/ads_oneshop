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
"""Starlark macro for extracting proto FileDescriptorSets from packages."""

load("@rules_python//python:py_binary.bzl", "py_binary")

def extract_proto_descriptors(name, packages, deps = [], out = None, **kwargs):
    """Extracts and serializes proto descriptors for the given packages.

    Args:
        name: Target name for the genrule.
        packages: List of Python package names to import.
        deps: List of py_library targets containing the GAPIC libraries.
        out: Optional output filename. Defaults to "<name>.descriptor_set".
        **kwargs: Extra arguments passed to genrule.
    """
    bin_name = name + "_extractor_bin"
    output_file = out or (
        name if name.endswith(".descriptor_set") else name + ".descriptor_set"
    )

    py_binary(
        name = bin_name,
        srcs = ["//build:extract_descriptors.py"],
        main = "//build:extract_descriptors.py",
        deps = [
            "//build:extract_descriptors_lib",
        ] + deps,
        visibility = ["//visibility:private"],
    )

    pkg_flags = " ".join(["--packages " + p for p in packages])

    native.genrule(
        name = name,
        outs = [output_file],
        cmd = "$(execpath :" + bin_name + ") " + pkg_flags + " --output $@",
        tools = [":" + bin_name],
        **kwargs
    )
