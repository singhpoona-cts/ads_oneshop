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

  def test_service_type_aggregation(self):
    svc = ma.AccountService(account_aggregation=ma.AccountAggregation())
    self.assertEqual(
        merchant_accounts._service_type(  # pylint: disable=protected-access
            svc),
        'ACCOUNT_AGGREGATION')

  def test_service_type_campaigns_management(self):
    svc = ma.AccountService(campaigns_management=ma.CampaignsManagement())
    self.assertEqual(
        merchant_accounts._service_type(  # pylint: disable=protected-access
            svc),
        'CAMPAIGNS_MANAGEMENT')

  def test_service_type_unset(self):
    self.assertEqual(
        merchant_accounts._service_type(  # pylint: disable=protected-access
            ma.AccountService()),
        '')

  def test_serialize_record_converts_to_dict_correctly(self):
    """Verifies that _serialize_record correctly transforms AccountRecord."""
    record = merchant_accounts.AccountRecord(
        account_id='123',
        account_name='Test Name',
        adult_content=False,
        test_account=True,
        time_zone=None,
        language_code='en',
        is_advanced=False,
        parent_account='456',
        homepage=ma.Homepage(uri='https://example.com', claimed=True),
        business_info=None,
        business_identity=None,
        automatic_improvements=None,
        users=[],
        account_relationships=[],
        account_services=[
            ma.AccountService(account_aggregation=ma.AccountAggregation())
        ],
    )
    serialized = merchant_accounts._serialize_record(record)  # pylint: disable=protected-access
    self.assertEqual(serialized['account_id'], '123')
    self.assertEqual(serialized['account_name'], 'Test Name')
    self.assertIsNone(serialized['time_zone'])
    self.assertEqual(serialized['homepage']['uri'], 'https://example.com')
    self.assertEqual(serialized['homepage']['claimed'], True)
    self.assertEqual(serialized['account_services'][0]['service_type'],
                     'ACCOUNT_AGGREGATION'
                     )


if __name__ == '__main__':
  absltest.main()
