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
"""Common utilities and shared constants for Merchant Center downloaders."""

from typing import Any
from google.protobuf import json_format

# Key stamped onto every row so downstream code knows the source account.
METADATA_KEY = 'downloaderMetadata'


def to_dict(msg: Any) -> Any:
  """Proto -> dict in native v1 shape.

  Args:
    msg: The protobuf message to convert into a dictionary.

  Returns:
    A dictionary representation of the proto message in native v1 shape with
    snake_case keys and string enum names, or None if the input message is
    None.
  """
  if msg is None:
    return None
  # Extract the underlying native protobuf if it's a proto-plus wrapper
  pb_msg = type(msg).pb(msg) if hasattr(type(msg), 'pb') else msg
  return json_format.MessageToDict(
      pb_msg,
      preserving_proto_field_name=True,
      use_integers_for_enums=False,
  )
