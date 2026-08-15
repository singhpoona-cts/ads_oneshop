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

from unittest import mock

from absl.testing import absltest
from acit import merchant_accounts
from google.api_core import exceptions as gax_exceptions
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

  def test_discover_topology_standalone_with_subaccounts_permission_denied(
      self,
  ):
    """Standalone accounts 403 on list_sub_accounts but succeed get_account."""
    clients = mock.MagicMock()
    clients.accounts.account_path.side_effect = lambda x: f'accounts/{x}'

    base_account = ma.Account(
        name='accounts/123', account_name='Standalone Inc'
    )
    clients.accounts.get_account.return_value = base_account
    clients.accounts.list_sub_accounts.side_effect = (
        gax_exceptions.PermissionDenied('Not MCA')
    )

    aggs, standalones, leaf_to_parent, base_accounts_by_id = (
        merchant_accounts.discover_topology(clients, ['123'])
    )

    self.assertEqual(aggs, set())
    self.assertEqual(standalones, {'123'})
    self.assertEqual(leaf_to_parent, {})
    self.assertEqual(base_accounts_by_id, {'123': base_account})

  def test_discover_topology_inaccessible_account_is_skipped(self):
    """Inaccessible accounts fail get_account and must be skipped."""
    clients = mock.MagicMock()
    clients.accounts.account_path.side_effect = lambda x: f'accounts/{x}'
    clients.accounts.get_account.side_effect = (
        gax_exceptions.PermissionDenied('Forbidden')
    )

    aggs, standalones, leaf_to_parent, base_accounts_by_id = (
        merchant_accounts.discover_topology(clients, ['999'])
    )

    self.assertEqual(aggs, set())
    self.assertEqual(standalones, set())
    self.assertEqual(leaf_to_parent, {})
    self.assertEqual(base_accounts_by_id, {})

  def test_discover_topology_advanced_mca_account(self):
    """Advanced accounts return sub-accounts and are aggregators."""
    clients = mock.MagicMock()
    clients.accounts.account_path.side_effect = lambda x: f'accounts/{x}'

    parent_account = ma.Account(name='accounts/100', account_name='Parent MCA')
    child_1 = ma.Account(
        name='accounts/101', account_id=101, account_name='Child 1'
    )
    child_2 = ma.Account(
        name='accounts/102', account_id=102, account_name='Child 2'
    )

    clients.accounts.get_account.return_value = parent_account
    clients.accounts.list_sub_accounts.return_value = [child_1, child_2]

    aggs, standalones, leaf_to_parent, base_accounts_by_id = (
        merchant_accounts.discover_topology(clients, ['100'])
    )

    self.assertEqual(aggs, {'100'})
    self.assertEqual(standalones, set())
    self.assertEqual(leaf_to_parent, {'101': '100', '102': '100'})
    self.assertEqual(
        base_accounts_by_id,
        {'100': parent_account, '101': child_1, '102': child_2},
    )


if __name__ == '__main__':
  absltest.main()
