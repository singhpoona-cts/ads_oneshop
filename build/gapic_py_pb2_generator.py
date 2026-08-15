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
"""Generates Python *_pb2.py files compatible with GAPIC proto-plus packages.

When protoc compiles a .proto file that imports protos from Google API / GAPIC
client libraries (e.g. `.../merchant_accounts_v1/types/accounts.proto`),
protoc's Python backend emits `from <path> import <file>_pb2` statements.

However, Google GAPIC client libraries (generated via gapic-generator-python
using proto-plus) do not ship *_pb2.py modules; they expose Python classes
directly in their package namespace and register their file descriptors into
the singleton `descriptor_pool.Default()` upon module import.

This tool runs protoc with `--descriptor_set_in` to compile proto definitions,
injects a preamble that imports specified GAPIC packages (ensuring all
imported file descriptors are pre-loaded in the descriptor pool), and comments
out the dangling `*_pb2` imports for those GAPIC namespaces so the generated
module can be cleanly imported in Python.
"""

import argparse
import os
import subprocess
import sys
from typing import Sequence


def _parse_list_arg(val: str | Sequence[str] | None) -> list[str]:
  """Parses arguments passed as repeated flags or comma/space-separated."""
  if not val:
    return []
  if isinstance(val, str):
    items = [item.strip() for item in val.replace(",", " ").split()]
    return [item for item in items if item]
  result = []
  for entry in val:
    items = [item.strip() for item in entry.replace(",", " ").split()]
    result.extend([item for item in items if item])
  return result


def main() -> None:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
      "--protoc", required=True, help="Path to protoc binary executable."
  )
  parser.add_argument(
      "--proto_path",
      required=True,
      help="Root directory path for proto resolution.",
  )
  parser.add_argument(
      "--descriptor_set_in",
      required=True,
      help="Colon-separated list of FileDescriptorSet binary files.",
  )
  parser.add_argument(
      "--proto_file",
      required=True,
      help="Path to input .proto file to compile.",
  )
  parser.add_argument(
      "--out_dir",
      required=True,
      help="Output directory for generated python code.",
  )
  parser.add_argument(
      "--target_py",
      required=True,
      help="Path to final output *_pb2.py file.",
  )
  parser.add_argument(
      "--gapic_packages",
      action="append",
      default=[],
      help=(
          "GAPIC Python package names to import in preamble (repeated or"
          " comma-separated)."
      ),
  )
  parser.add_argument(
      "--gapic_prefixes",
      action="append",
      default=[],
      help=(
          "Module prefixes whose *_pb2 imports should be commented out"
          " (repeated or comma-separated)."
      ),
  )
  args = parser.parse_args()

  gapic_packages = _parse_list_arg(args.gapic_packages)
  gapic_prefixes = _parse_list_arg(args.gapic_prefixes)

  # Derive prefixes from package names (e.g. "google.shopping.") if omitted
  if not gapic_prefixes:
    for pkg in gapic_packages:
      parts = pkg.split(".")
      if len(parts) >= 2:
        gapic_prefixes.append(".".join(parts[:2]) + ".")
      else:
        gapic_prefixes.append(pkg + ".")
    gapic_prefixes = sorted(set(gapic_prefixes))

  cmd = [
      args.protoc,
      f"--proto_path={args.proto_path}",
      f"--descriptor_set_in={args.descriptor_set_in}",
      f"--python_out={args.out_dir}",
      args.proto_file,
  ]
  try:
    subprocess.run(cmd, capture_output=True, text=True, check=True)
  except subprocess.CalledProcessError as e:
    sys.stderr.write(
        f"protoc failed (exit {e.returncode}):\n{e.stderr}\n"
    )
    sys.exit(e.returncode)

  if not os.path.exists(args.target_py):
    sys.stderr.write(
        f"Expected generated file not found at {args.target_py}\n"
    )
    sys.exit(1)

  with open(args.target_py, "r", encoding="utf-8") as f:
    lines = f.readlines()

  # Build preamble to pre-load GAPIC packages
  preamble_lines = [
      "# Pre-load GAPIC packages to register descriptors in"
      " descriptor_pool.Default()\n",
  ]
  for pkg in gapic_packages:
    preamble_lines.append(f"import {pkg}\n")

  new_lines = preamble_lines
  for line in lines:
    is_gapic_import = any(
        line.startswith(f"from {prefix}") for prefix in gapic_prefixes
    )
    if is_gapic_import and "_pb2" in line:
      new_lines.append(f"# {line}")
    else:
      new_lines.append(line)

  with open(args.target_py, "w", encoding="utf-8") as f:
    f.writelines(new_lines)


if __name__ == "__main__":
  main()
