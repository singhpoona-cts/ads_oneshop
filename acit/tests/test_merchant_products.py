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
"""Tests for the Merchant API v1 products ingestion helpers."""

import json
from unittest import mock

from absl.testing import absltest
from acit import merchant_products
from etils import epath
from google.shopping import merchant_products_v1 as mp


class MerchantProductsTest(absltest.TestCase):
  """Unit tests for the Merchant API v1 products ingestion helpers."""

  def test_metadata_key(self):
    self.assertEqual(merchant_products.METADATA_KEY, 'downloaderMetadata')


class _FakeProductsClient:
  """Minimal stand-in for ProductsServiceClient.

  That counts pager consumption.
  """

  def __init__(self, products):
    self._products = products
    self.consumed = 0

  def list_products(self, request):
    del request  # Unused; the fake ignores paging parameters.

    def _pager():
      for product in self._products:
        self.consumed += 1
        yield product

    return _pager()


class ListLeafProductsStreamingTest(absltest.TestCase):
  """A large merchant's catalog.

  It must never be materialized in memory at once.
  """

  def _products(self, n):
    return [mp.Product(offer_id=f'SKU{i}') for i in range(n)]

  def test_returns_a_lazy_generator(self):
    client = _FakeProductsClient(self._products(3))
    result = (
        merchant_products.  # pylint: disable=protected-access
        _list_account_products(client, '123'))
    # Nothing is fetched or converted until the caller starts iterating.
    self.assertEqual(client.consumed, 0)
    first = next(iter(result))
    self.assertEqual(first.offer_id, 'SKU0')
    self.assertEqual(client.consumed, 1)

  def test_yields_every_product(self):
    client = _FakeProductsClient(self._products(5))
    rows = list(merchant_products.  # pylint: disable=protected-access
                _list_account_products(client, '123'))
    self.assertLen(rows, 5)
    self.assertEqual([r.offer_id for r in rows],
                     [f'SKU{i}' for i in range(5)])

  def test_empty_account_yields_nothing(self):
    client = _FakeProductsClient([])
    self.assertEmpty(
        list(merchant_products.  # pylint: disable=protected-access
             _list_account_products(client, '123')))

  def test_download_products_streams_to_disk(self):
    """`_process` must write while iterating, not after collecting."""
    client = _FakeProductsClient(self._products(4))
    mc_path = epath.Path(self.create_tempdir().full_path)

    with mock.patch.object(mp,
                           'ProductsServiceClient',
                           return_value=client):
      merchant_products.download_products(None, ['123'], mc_path)

    written = (mc_path / '123' / 'products' / 'rows.jsonlines').read_text()
    rows = [json.loads(line) for line in written.splitlines()]
    self.assertEqual([r['offer_id'] for r in rows],
                     [f'SKU{i}' for i in range(4)])


if __name__ == '__main__':
  absltest.main()
