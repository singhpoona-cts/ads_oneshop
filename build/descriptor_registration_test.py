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
"""Tests that descriptor pool registers GAPIC and schema descriptors."""

import os
from absl.testing import absltest
from acit.api.v0.storage import schema_pb2
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import json_format
from python.runfiles import runfiles


class DescriptorRegistrationTest(absltest.TestCase):
  """Validates descriptor registration and side-by-side co-existence."""

  def test_gapic_descriptors_registered_in_singleton_pool(self):
    pool = descriptor_pool.Default()

    account_msg_desc = pool.FindMessageTypeByName(
        "google.shopping.merchant.accounts.v1.Account"
    )
    self.assertIsNotNone(account_msg_desc)

    product_msg_desc = pool.FindMessageTypeByName(
        "google.shopping.merchant.products.v1.Product"
    )
    self.assertIsNotNone(product_msg_desc)

    shipping_msg_desc = pool.FindMessageTypeByName(
        "google.shopping.merchant.accounts.v1.ShippingSettings"
    )
    self.assertIsNotNone(shipping_msg_desc)

  def test_schema_pb2_coexists_with_gapic_clients(self):
    """Asserts schema_pb2 can be imported and instantiated without errors."""
    account = schema_pb2.Account()
    account.account_id = 12345
    account.account_name = "Test Merchant"

    svc_row = schema_pb2.AccountServiceRow()
    svc_row.name = "accounts/12345/services/67890"
    svc_row.service_type = "ACCOUNT_AGGREGATION"
    account.account_services.append(svc_row)

    self.assertEqual(account.account_id, 12345)
    self.assertEqual(account.account_name, "Test Merchant")
    self.assertEqual(
        account.account_services[0].service_type, "ACCOUNT_AGGREGATION"
    )

    # Serialize to and from JSON
    json_str = json_format.MessageToJson(account)
    self.assertIn('"accountId": "12345"', json_str)
    self.assertIn('"accountName": "Test Merchant"', json_str)

  def test_descriptor_set_artifact_validity(self):
    fds = descriptor_pb2.FileDescriptorSet()
    r = runfiles.Create()
    path = r.Rlocation("ads_oneshop/build/merchant_api.descriptor_set")
    self.assertTrue(os.path.exists(path), f"Descriptor set not found at {path}")
    with open(path, "rb") as f:
      fds.ParseFromString(f.read())

    file_names = {f.name for f in fds.file}
    self.assertIn(
        "google/shopping/merchant_accounts_v1/types/accounts.proto",
        file_names,
    )
    self.assertIn(
        "google/shopping/merchant_products_v1/types/products.proto",
        file_names,
    )
    self.assertIn(
        "google/shopping/merchant_accounts_v1/types/shippingsettings.proto",
        file_names,
    )
    self.assertIn(
        "google/shopping/merchant_accounts_v1/types/homepage.proto",
        file_names,
    )


if __name__ == "__main__":
  absltest.main()
