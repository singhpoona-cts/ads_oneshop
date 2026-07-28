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

from absl.testing import absltest
from acit import merchant_shipping
from google.shopping import merchant_accounts_v1 as ma


def _service(**kwargs):
  return ma.Service(**kwargs)


class MerchantShippingTest(absltest.TestCase):
  """Unit tests for the Merchant API v1 shipping-settings ingestion helpers."""
  def test_to_dict_renders_enums_as_strings(self):
    """Native v1 shape must use enum *names*, not integers, for BigQuery STRING."""
    settings = ma.ShippingSettings(
        services=[_service(
            service_name='FedEx',
            shipment_type='DELIVERY',
            delivery_time=ma.DeliveryTime(
                min_transit_days=1,
                max_transit_days=5,
                handling_business_day_config=ma.BusinessDayConfig(
                    business_days=['MONDAY', 'FRIDAY']),
            ),
        )],
    )
    d = merchant_shipping._to_dict(settings) # pylint: disable=protected-access
    svc = d['services'][0]
    self.assertEqual(svc['shipment_type'], 'DELIVERY')
    self.assertEqual(
        svc['delivery_time']['handling_business_day_config']['business_days'],
        ['MONDAY', 'FRIDAY'])

  def test_to_dict_uses_snake_case_keys(self):
    settings = ma.ShippingSettings(
        services=[_service(
            service_name='Main',
            delivery_countries=['US', 'IL'],
            currency_code='USD',
            delivery_time=ma.DeliveryTime(min_transit_days=2,
                                          max_transit_days=4),
        )],
    )
    svc = (merchant_shipping. # pylint: disable=protected-access
           _to_dict(settings)['services'][0])
    self.assertEqual(svc['service_name'], 'Main')
    self.assertEqual(svc['delivery_countries'], ['US', 'IL'])
    self.assertEqual(svc['currency_code'], 'USD')
    self.assertEqual(svc['delivery_time']['min_transit_days'], 2)

  def test_free_flat_rate_amount_micros_present(self):
    """A zero flat rate must keep `amount_micros` (so SQL `= 0` matches)."""
    settings = ma.ShippingSettings(
        services=[_service(
            service_name='Free',
            rate_groups=[ma.RateGroup(
                single_value=ma.Value(
                    flat_rate={'amount_micros': 0, 'currency_code': 'USD'}),
            )],
        )],
    )
    rg = (merchant_shipping. # pylint: disable=protected-access
          _to_dict(settings)['services'][0]['rate_groups'][0])
    flat = rg['single_value']['flat_rate']
    # proto-plus renders int64 as a string; 
    # the zero must be present, not dropped.
    self.assertIn('amount_micros', flat)
    self.assertEqual(int(flat['amount_micros']), 0)
    self.assertEqual(flat['currency_code'], 'USD')

  def test_paid_flat_rate_micros_scale(self):
    """A paid rate is expressed in micros (1 unit = 1e6)."""
    settings = ma.ShippingSettings(
        services=[_service(
            rate_groups=[ma.RateGroup(
                single_value=ma.Value(
                    flat_rate={'amount_micros': 5_000_000,
                               'currency_code': 'USD'}),
            )],
        )],
    )
    flat = (merchant_shipping. # pylint: disable=protected-access
            _to_dict(settings)['services'][0]
            ['rate_groups'][0]['single_value']['flat_rate'])
    self.assertEqual(int(flat['amount_micros']), 5_000_000)

  def test_main_table_cell_flat_rate(self):
    """Table cells are `Value` messages carrying `flat_rate`."""
    settings = ma.ShippingSettings(
        services=[_service(
            rate_groups=[ma.RateGroup(
                main_table=ma.Table(
                    rows=[ma.Row(cells=[ma.Value(
                        flat_rate={'amount_micros': 0,
                                   'currency_code': 'USD'})])],
                ),
            )],
        )],
    )
    cell = (merchant_shipping. # pylint: disable=protected-access
            _to_dict(settings)['services'][0]
            ['rate_groups'][0]['main_table']['rows'][0]['cells'][0])
    self.assertEqual(int(cell['flat_rate']['amount_micros']), 0)

  def test_metadata_key(self):
    self.assertEqual(merchant_shipping.METADATA_KEY, 'downloaderMetadata')


if __name__ == '__main__':
  absltest.main()
