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
"""Merchant Center shipping-settings ingestion via the Merchant API (stable v1).

This replaces the old Content API `shippingsettings.get`
(per account) + `shippingsettings.list` (MCA
roll-down) pulls as part of the Content API -> Merchant API migration.

In the Merchant API, account-level shipping config lives in
**ShippingSettings**, a singleton per account fetched via
``ShippingSettingsServiceClient.get_shipping_settings(
name="accounts/{id}/shippingSettings")``. There is no MCA roll-down `list`, and
the aggregator itself is NOT a valid target (it returns ``PermissionDenied`` --
"only subaccounts and standalone accounts"). So we fetch per (sub)account
directly and never query the aggregator.

Output is written in the NATIVE v1 shape (snake_case keys,
enum NAMES as strings) as one FLAT record per account --
``{"account_id": <int>, "services": [<Service>, ...], "warehouses": [...],
"etag": <str>}`` -- to
``<mc_path>/<account_id>/shippingsettings/rows.jsonlines``, preserving
the existing BigQuery glob
(``merchant_center/*/shippingsettings/rows.jsonlines``). The old
``{settings, children[]}`` envelope is gone; downstream SQL recovers the
MCA<->child relationship from the (already migrated) `accounts` table.
"""

from concurrent import futures
import json
from typing import Iterable, Tuple

from absl import logging
from acit.utils import METADATA_KEY
from acit.utils import to_dict as _to_dict
from etils import epath
from google.api_core import exceptions as gax_exceptions
from google.auth import credentials as _credentials
from google.shopping import merchant_accounts_v1 as ma


def _get_account_shipping_settings(
    client: ma.ShippingSettingsServiceClient, account_id: str
    ) -> ma.ShippingSettings | None:
  """Fetches the v1 shipping settings for one account.

  Returns a protobuf message, not a dict; the caller converts it via
  `utils.to_dict` on the way to disk.

  Args:
    client: The API client instance used to make the request.
    account_id: The ID of the account to query.

  Returns:
    A ShippingSettings protobuf message, or None if
    the account has no shipping settings (NotFound) or is not a valid target
    for this method (e.g., an aggregator or MCA resulting in PermissionDenied).
  """
  name = f'accounts/{account_id}/shippingSettings'
  try:
    return client.get_shipping_settings(
        request=ma.GetShippingSettingsRequest(name=name))
  except gax_exceptions.PermissionDenied:
    # Aggregators/MCAs are not valid targets -- "only subaccounts and standalone
    # accounts". The old Content API aggregator `get` returned 404 anyway.
    logging.info(
        'Shipping settings not accessible for %s '
        '(not a sub-/standalone account); skipping', account_id)
    return None
  except gax_exceptions.NotFound:
    # No shipping settings configured --
    # mirrors the old Content API 404 swallow.
    return None


def download_shipping_settings(
    credentials: _credentials.Credentials,
    account_ids: Iterable[str],
    mc_path: epath.Path, max_workers: int | None = None
    ) -> None:
  """Downloads shipping settings from Merchant API v1, one file per account.

  Args:
    credentials: Google credentials (same as used for the Ads/Content APIs).
    account_ids: iterable of sub-/standalone Merchant Center account IDs
    to pull.(Aggregators are skipped automatically --
    they are not valid targets.)
    mc_path: epath.Path to the merchant_center output directory.
    max_workers: max concurrent account downloads. Callers (namely `acit.py`)
      should pass this explicitly so it stays consistent with the other
      Merchant API ingestion stages.
  """
  client = ma.ShippingSettingsServiceClient(credentials=credentials)
  account_ids_list = list(account_ids)

  def _process(account_id: str) -> Tuple[str, int]:
    settings = _get_account_shipping_settings(client, account_id)
    # None (not accessible / no settings) -> write nothing; downstream MEX LEFT
    # JOINs default such accounts to "no account-level shipping".
    if settings is None:
      return account_id, 0
    settings_dict = _to_dict(settings)
    services = settings_dict.get('services', []) or []
    record = {
        'account_id': int(account_id),
        'services': services,
        'warehouses': settings_dict.get('warehouses', []) or [],
        'etag': settings_dict.get('etag'),
        METADATA_KEY: {'accountId': account_id},
    }
    output_file = (
        epath.Path(mc_path)
        / account_id
        / 'shippingsettings'
        / 'rows.jsonlines'
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open('w') as f:
      f.write(json.dumps(record) + '\n')
    return account_id, len(services)

  total = 0
  with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
    future_to_id = {ex.submit(_process, aid): aid for aid in account_ids_list}
    for done in futures.as_completed(future_to_id):
      account_id, n = done.result()  # surface exceptions
      total += n
      if n:
        logging.info('Wrote shipping settings (%d service(s)) for %s',
                     n, account_id)

  logging.info(
      'Merchant API shipping: %d account(s) queried, %d service(s) total',
      len(account_ids_list), total)
