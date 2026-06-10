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
"""Unit tests for the Merchant API v1 products ingestion helpers."""

from absl.testing import absltest
from acit import merchant_products
from google.shopping import merchant_products_v1 as mp
from google.shopping import type as shopping_type


class MerchantProductsTest(absltest.TestCase):

  def test_to_dict_renders_enums_as_strings(self):
    """Native v1 shape must use enum *names*, not integers, for BigQuery STRING."""
    product = mp.Product(
        offer_id='SKU1',
        content_language='en',
        feed_label='US',
        product_attributes=mp.ProductAttributes(
            availability=mp.Availability.IN_STOCK,
            condition=mp.Condition.NEW,
        ),
    )
    d = merchant_products._to_dict(product)
    self.assertEqual(d['product_attributes']['availability'], 'IN_STOCK')
    self.assertEqual(d['product_attributes']['condition'], 'NEW')

  def test_to_dict_uses_snake_case_keys(self):
    product = mp.Product(offer_id='SKU1', content_language='en')
    d = merchant_products._to_dict(product)
    self.assertEqual(d['offer_id'], 'SKU1')
    self.assertEqual(d['content_language'], 'en')

  def test_to_dict_price_is_micros(self):
    """v1 Price is {amount_micros, currency_code}, not {value, currency}."""
    product = mp.Product(
        product_attributes=mp.ProductAttributes(
            price=shopping_type.Price(amount_micros=6_000_000, currency_code='USD')
        )
    )
    d = merchant_products._to_dict(product)
    price = d['product_attributes']['price']
    # int64 fields render as strings in proto JSON.
    self.assertEqual(int(price['amount_micros']), 6_000_000)
    self.assertEqual(price['currency_code'], 'USD')

  def test_to_dict_status_reporting_context_enum(self):
    """Status `destination` is now the `reporting_context` enum name."""
    product = mp.Product(
        product_status=mp.ProductStatus(
            destination_statuses=[
                mp.ProductStatus.DestinationStatus(
                    reporting_context=shopping_type.ReportingContext.ReportingContextEnum.SHOPPING_ADS,
                    disapproved_countries=['US'],
                )
            ]
        )
    )
    d = merchant_products._to_dict(product)
    ds = d['product_status']['destination_statuses'][0]
    self.assertEqual(ds['reporting_context'], 'SHOPPING_ADS')
    self.assertEqual(ds['disapproved_countries'], ['US'])

  def test_metadata_key(self):
    self.assertEqual(merchant_products.METADATA_KEY, 'downloaderMetadata')


if __name__ == '__main__':
  absltest.main()
