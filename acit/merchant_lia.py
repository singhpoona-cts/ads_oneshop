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
"""Merchant Center omnichannel / LIA ingestion via the Merchant API (stable v1).

Phase 3 of the Content API -> Merchant API migration. Replaces the old Content
API `liasettings.get` (per account) + `liasettings.list` (MCA roll-down) pulls.

The Merchant API has no `liasettings` resource: the Local Inventory Ads /
local-storefront config now lives in **OmnichannelSettings**, listed per region
under each account via
``OmnichannelSettingsServiceClient.list_omnichannel_settings``. There is also no
MCA roll-down `list`, and the aggregator itself is NOT a valid parent for this
method (it returns ``PermissionDenied`` -- "only subaccounts and standalone
accounts"). So we pull per (sub)account directly and never query the aggregator.

Output is written in the NATIVE v1 shape 
(snake_case keys, enum NAMES as strings) as one FLAT record per account --
``{"account_id": <int>, "omnichannel_settings": [<OmnichannelSetting>, ...]}`` 
-- to ``<mc_path>/<account_id>/liasettings/rows.jsonlines``,
preserving the existing BigQuery glob 
(``merchant_center/*/liasettings/*.jsonlines``). The old
``{settings, children[]}`` envelope is gone; downstream SQL recovers the
MCA<->child relationship from the (already migrated) `accounts` table.
"""

from concurrent import futures
import json
import time

from absl import logging
from etils import epath
from google.api_core import exceptions as gax_exceptions
from google.shopping import merchant_accounts_v1 as ma

# Key stamped onto every row so downstream code knows the source account.
# Mirrors resource_downloader.METADATA_KEY.
# (account_id is also written at top level.)
METADATA_KEY = 'downloaderMetadata'

# The Merchant API v1 backend returns sporadic 500 INTERNAL / 503 errors; retry.
_TRANSIENT = (
    gax_exceptions.InternalServerError,
    gax_exceptions.ServiceUnavailable,
    gax_exceptions.DeadlineExceeded,
    gax_exceptions.TooManyRequests,
)

_PAGE_SIZE = 250
_MAX_WORKERS = 8


def _retry(fn, *, what, attempts=6):
  """Calls `fn` with exponential backoff on transient server errors."""
  delay = 1.0
  for i in range(1, attempts + 1):
    try:
      return fn()
    except _TRANSIENT as e:
      if i == attempts:
        raise
      logging.warning(
          'Transient %s on %s (attempt %d/%d); retrying in %.0fs',
          type(e).__name__, what, i, attempts, delay,
      )
      time.sleep(delay)
      delay = min(delay * 2, 16.0)


def _to_dict(msg):
  """Proto -> dict in native v1 shape: snake_case keys,enum *names* (not ints)."""
  return type(msg).to_dict(msg, use_integers_for_enums=False)


def _list_account_omnichannel(client, account_id):
  """Lists v1 omnichannel settings for one account, as native-shape dicts.

  Args:
    client: The API client instance used to make the request.
    account_id: The string or integer ID of the account to query.

  Returns:
    A (possibly empty) list of per-region OmnichannelSetting dicts, or None if
    the account is not a valid parent for this method (e.g., an aggregator
    or MCA resulting in PermissionDenied).
  """
  parent = f'accounts/{account_id}'
  try:
    pager = _retry(
        lambda: client.list_omnichannel_settings(
            request=ma.ListOmnichannelSettingsRequest(
                parent=parent, page_size=_PAGE_SIZE)),
        what=f'list_omnichannel_settings:{account_id}',
    )
  except gax_exceptions.PermissionDenied:
    # Aggregators/MCAs are not valid parents -- "only subaccounts and standalone
    # accounts". The old Content API aggregator `get` returned empty anyway.
    logging.info(
        'Omnichannel settings not accessible for %s '
        '(not a sub-/standalone account); skipping', account_id)
    return None
  except gax_exceptions.NotFound:
    return []

  rows = []
  it = iter(pager)
  while True:
    try:
      setting = _retry(
          lambda: next(it, None),
          what=f'list_omnichannel_settings_page:{account_id}')
    except StopIteration:
      break
    if setting is None:
      break
    rows.append(_to_dict(setting))
  return rows


def download_omnichannel_settings(
    credentials, account_ids, mc_path, max_workers=_MAX_WORKERS):
  """Downloads omnichannel/LIA settings from Merchant API v1,
  one file per account.

  Args:
    credentials: Google credentials (same as used for the Ads/Content APIs).
    account_ids: iterable of sub-/standalone Merchant Center account
    IDs to pull. (Aggregators are skipped automatically 
    -- they are not valid parents.)
    mc_path: epath.Path to the merchant_center output directory.
    max_workers: max concurrent account downloads. Callers (namely `acit.py`)
      should pass this explicitly so it stays consistent with the other
      Merchant API ingestion stages.
  """
  client = ma.OmnichannelSettingsServiceClient(credentials=credentials)
  account_ids = list(account_ids)

  def _process(account_id):
    rows = _list_account_omnichannel(client, account_id)
    # None (not accessible) or [] (no settings) -> write nothing; downstream MEX
    # LEFT JOINs default such accounts to "not implemented".
    if not rows:
      return account_id, 0
    record = {
        'account_id': int(account_id),
        'omnichannel_settings': rows,
        METADATA_KEY: {'accountId': account_id},
    }
    output_file = (
        epath.Path(mc_path) / account_id / 'liasettings' / 'rows.jsonlines')
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open('w') as f:
      f.write(json.dumps(record) + '\n')
    return account_id, len(rows)

  total = 0
  with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
    future_to_id = {ex.submit(_process, aid): aid for aid in account_ids}
    for done in futures.as_completed(future_to_id):
      account_id, n = done.result()  # surface exceptions
      total += n
      if n:
        logging.info('Wrote %d omnichannel setting(s) for %s', n, account_id)

  logging.info(
      'Merchant API omnichannel: %d account(s) queried, %d setting(s) total',
      len(account_ids), total)
