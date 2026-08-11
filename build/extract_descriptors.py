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
"""Extracts and serializes Protobuf FileDescriptorSets from GAPIC packages."""

import importlib
import inspect
import os
import pkgutil
import sys
from typing import Sequence

from absl import app
from absl import flags
from absl import logging
from google.protobuf import descriptor
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool

FLAGS = flags.FLAGS

flags.DEFINE_multi_string(
    "packages",
    None,
    "Python package names to dynamically load and extract descriptors from.",
    short_name="p",
    required=False,
)
flags.DEFINE_string(
    "output",
    None,
    "Output path for the serialized FileDescriptorSet binary file.",
    short_name="o",
    required=True,
)


def _load_all_submodules(package_name: str) -> None:
  """Imports the given package and all its submodules recursively."""
  try:
    mod = importlib.import_module(package_name)
  except ImportError as e:
    logging.warning("Failed to import package %s: %s", package_name, e)
    return

  if hasattr(mod, "__path__"):
    for _, subname, _ in pkgutil.walk_packages(
        mod.__path__, prefix=f"{package_name}."
    ):
      try:
        importlib.import_module(subname)
      except Exception as e:  # pylint: disable=broad-except
        logging.debug("Could not import submodule %s: %s", subname, e)


def _canonical_name(name: str) -> str:
  """Preserves the native descriptor file name registered by GAPIC."""
  return name


def _collect_file_descriptors(
    package_names: Sequence[str],
) -> list[descriptor.FileDescriptor]:
  """Collects all FileDescriptors registered by importing packages."""
  for pkg in package_names:
    _load_all_submodules(pkg)

  pool = descriptor_pool.Default()
  collected_fds: dict[str, descriptor.FileDescriptor] = {}

  def _add_fd_and_deps(fd: descriptor.FileDescriptor):
    if not fd or fd.name in collected_fds:
      return
    collected_fds[fd.name] = fd
    for dep in fd.dependencies:
      _add_fd_and_deps(dep)

  def _inspect_desc(desc):
    if isinstance(desc, descriptor.Descriptor):
      _add_fd_and_deps(desc.file)
    elif isinstance(desc, descriptor.FileDescriptor):
      _add_fd_and_deps(desc)
    elif isinstance(desc, descriptor.EnumDescriptor):
      _add_fd_and_deps(desc.file)
    elif isinstance(desc, descriptor.ServiceDescriptor):
      _add_fd_and_deps(desc.file)

  def _inspect_obj(obj):
    if hasattr(obj, "pb") and callable(obj.pb):
      try:
        pb_cls = obj.pb()
        if hasattr(pb_cls, "DESCRIPTOR"):
          _inspect_desc(pb_cls.DESCRIPTOR)
      except Exception:  # pylint: disable=broad-except
        pass

    try:
      desc = getattr(obj, "DESCRIPTOR", None)
    except Exception:  # pylint: disable=broad-except
      desc = None

    if desc:
      _inspect_desc(desc)

  # Inspect loaded modules matching requested packages or namespaces
  for mod_name, mod in list(sys.modules.items()):
    if not mod or not (
        mod_name.startswith("google.")
        or any(mod_name.startswith(p) for p in package_names)
    ):
      continue
    try:
      members = inspect.getmembers(mod)
    except Exception:  # pylint: disable=broad-except
      continue

    for _, obj in members:
      _inspect_obj(obj)

  # Check all collected files in pool to resolve further dependencies
  for name in list(collected_fds):
    try:
      fd = pool.FindFileByName(name)
      _add_fd_and_deps(fd)
    except KeyError:
      pass

  return list(collected_fds.values())


def main(argv: Sequence[str]) -> None:
  packages: list[str] = []
  if FLAGS.packages:
    packages.extend(FLAGS.packages)
  if len(argv) > 1:
    packages.extend(argv[1:])

  if not packages:
    raise app.UsageError(
        "At least one package name must be specified via --packages or"
        " positional args."
    )

  logging.info("Extracting descriptors for packages: %s", packages)
  file_descriptors = _collect_file_descriptors(packages)
  logging.info("Found %d file descriptors.", len(file_descriptors))

  # Build FileDescriptorSet in topological dependency order
  visited = set()
  ordered_fds: list[descriptor.FileDescriptor] = []

  def _topo_sort(fd: descriptor.FileDescriptor):
    if fd.name in visited:
      return
    visited.add(fd.name)
    for dep in fd.dependencies:
      _topo_sort(dep)
    ordered_fds.append(fd)

  for fd in file_descriptors:
    _topo_sort(fd)

  # Map of original -> canonical name
  remap = {fd.name: _canonical_name(fd.name) for fd in ordered_fds}

  descriptor_set = descriptor_pb2.FileDescriptorSet()
  added_names = set()

  for fd in ordered_fds:
    cname = remap[fd.name]
    if cname not in added_names:
      proto = descriptor_pb2.FileDescriptorProto()
      fd.CopyToProto(proto)
      proto.name = cname
      for i, dep in enumerate(proto.dependency):
        if dep in remap:
          proto.dependency[i] = remap[dep]
      descriptor_set.file.append(proto)
      added_names.add(cname)

  os.makedirs(os.path.dirname(os.path.abspath(FLAGS.output)), exist_ok=True)
  with open(FLAGS.output, "wb") as f:
    f.write(descriptor_set.SerializeToString())

  logging.info(
      "Wrote %d descriptors to %s (%d bytes)",
      len(descriptor_set.file),
      FLAGS.output,
      len(descriptor_set.SerializeToString()),
  )


if __name__ == "__main__":
  app.run(main)
