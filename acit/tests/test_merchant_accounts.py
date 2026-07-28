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

    def test_to_dict_renders_enums_as_strings(self):
        """Native v1 shape must use enum *names*,
        not integers, for BigQuery STRING."""

        bi = ma.BusinessIdentity(promotions_consent=(
            ma.BusinessIdentity.PromotionsConsent.PROMOTIONS_CONSENT_GIVEN))
        d = merchant_accounts._to_dict(bi)  # pylint: disable=protected-access
        self.assertEqual(d['promotions_consent'], 'PROMOTIONS_CONSENT_GIVEN')

    def test_to_dict_uses_snake_case_keys(self):
        account = ma.Account(account_id=123, account_name='Test')
        d = merchant_accounts._to_dict(account)  # pylint: disable=protected-access
        self.assertEqual(d['account_name'], 'Test')

    def test_to_dict_none(self):
        self.assertIsNone(merchant_accounts._to_dict(None))  # pylint: disable=protected-access

    def test_service_type_aggregation(self):
        svc = ma.AccountService(account_aggregation=ma.AccountAggregation())
        self.assertEqual(
            merchant_accounts._service_type(svc),  # pylint: disable=protected-access
            'ACCOUNT_AGGREGATION')

    def test_service_type_campaigns_management(self):
        svc = ma.AccountService(campaigns_management=ma.CampaignsManagement())
        self.assertEqual(
            merchant_accounts._service_type(svc),  # pylint: disable=protected-access
            'CAMPAIGNS_MANAGEMENT')

    def test_service_type_unset(self):
        self.assertEqual(
            merchant_accounts._service_type(ma.AccountService()),  # pylint: disable=protected-access
            '')


if __name__ == '__main__':
    absltest.main()
