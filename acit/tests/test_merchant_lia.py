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
"""Tests for the Merchant API v1 omnichannel/LIA ingestion helpers."""

from absl.testing import absltest
from acit import merchant_lia


class MerchantLiaTest(absltest.TestCase):
  """Unit tests for the Merchant API v1 omnichannel/LIA ingestion helpers."""

  def test_metadata_key(self):
    self.assertEqual(merchant_lia.METADATA_KEY, 'downloaderMetadata')


if __name__ == '__main__':
  absltest.main()
