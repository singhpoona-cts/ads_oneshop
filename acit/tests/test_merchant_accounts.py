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
"""Unit tests for the Merchant API v1 accounts ingestion helpers."""

from absl.testing import absltest
from acit import merchant_accounts
from google.shopping import merchant_accounts_v1 as ma


class MerchantAccountsTest(absltest.TestCase):
  """Tests for Merchant API v1 accounts ingestion helper functions."""

  # pylint: disable=protected-access

  def test_service_type_name_aggregation(self):
    svc = ma.AccountService(account_aggregation=ma.AccountAggregation())
    self.assertEqual('ACCOUNT_AGGREGATION',
                     merchant_accounts._service_type_name(svc))

  def test_service_type_name_campaigns_management(self):
    svc = ma.AccountService(campaigns_management=ma.CampaignsManagement())
    self.assertEqual('CAMPAIGNS_MANAGEMENT',
                     merchant_accounts._service_type_name(svc))

  def test_service_type_name_unset(self):
    self.assertEqual(
        '', merchant_accounts._service_type_name(ma.AccountService()))

  def test_to_service_row_flattens_oneof(self):
    """The empty `service_type` oneof must collapse to one STRING field.

    See `AccountServiceRow` in schema.proto: the v1 oneof members are all empty
    messages, so embedding them would give BigQuery six always-empty STRUCTs.
    """
    svc = ma.AccountService(
        name='accounts/123/services/456',
        provider='accounts/789',
        provider_display_name='Provider',
        external_account_id='ext-1',
        account_aggregation=ma.AccountAggregation(),
    )
    row = merchant_accounts._to_service_row(svc)

    self.assertEqual('ACCOUNT_AGGREGATION', row.service_type)
    self.assertEqual('accounts/123/services/456', row.name)
    self.assertEqual('accounts/789', row.provider)
    self.assertEqual('ext-1', row.external_account_id)

  def test_to_service_row_without_service_type(self):
    """An AccountService with no oneof set yields an empty service_type."""
    row = merchant_accounts._to_service_row(
        ma.AccountService(name='accounts/123/services/456'))
    self.assertEqual('', row.service_type)


if __name__ == '__main__':
  absltest.main()
