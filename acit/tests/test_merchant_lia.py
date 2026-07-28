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
from acit import merchant_lia
from google.shopping import merchant_accounts_v1 as ma


class MerchantLiaTest(absltest.TestCase):
  """Unit tests for the Merchant API v1 omnichannel/LIA ingestion helpers."""
  def test_to_dict_renders_enums_as_strings(self):
    """Native v1 shape must use enum *names*, not integers, for BigQuery STRING."""
    setting = ma.OmnichannelSetting(
        region_code='IN',
        lsf_type='GHLSF',
        in_stock=ma.InStock(state='ACTIVE'),
    )
    d = merchant_lia._to_dict(setting) # pylint: disable=protected-access
    self.assertEqual(d['lsf_type'], 'GHLSF')
    self.assertEqual(d['in_stock']['state'], 'ACTIVE')

  def test_to_dict_uses_snake_case_keys(self):
    setting = ma.OmnichannelSetting(
        region_code='US',
        inventory_verification=ma.InventoryVerification(
            contact='Jane', contact_email='jane@example.com'),
    )
    d = merchant_lia._to_dict(setting) # pylint: disable=protected-access
    self.assertEqual(d['region_code'], 'US')
    self.assertEqual(d['inventory_verification']['contact'], 'Jane')
    self.assertEqual(d['inventory_verification']['contact_email'],
                     'jane@example.com')

  def test_to_dict_unset_state_is_unspecified(self):
    """An unset omnichannel sub-feature renders as the *_UNSPECIFIED enum name.

    This is the verified mapping of the old Content API `inactive` status.
    """
    setting = ma.OmnichannelSetting(region_code='IN')
    d = merchant_lia._to_dict(setting) # pylint: disable=protected-access
    self.assertEqual(d['lsf_type'], 'LSF_TYPE_UNSPECIFIED')

  def test_to_dict_lfp_link(self):
    """`posDataProvider` becomes `lfp_link` (provider resource + external id)."""
    setting = ma.OmnichannelSetting(
        region_code='IN',
        lfp_link=ma.LfpLink(
            lfp_provider='accounts/123/omnichannelSettings/IN/lfpProviders/456',
            external_account_id='ext-1',
            state='ACTIVE',
        ),
    )
    d = merchant_lia._to_dict(setting) # pylint: disable=protected-access
    self.assertEqual(d['lfp_link']['external_account_id'], 'ext-1')
    self.assertEqual(d['lfp_link']['state'], 'ACTIVE')

  def test_metadata_key(self):
    self.assertEqual(merchant_lia.METADATA_KEY, 'downloaderMetadata')


if __name__ == '__main__':
  absltest.main()
