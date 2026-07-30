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
"""Merchant Center *account* ingestion via the Merchant API (stable v1).

Phase 1 of the Content API -> Merchant API migration. This replaces the old
Content API `accounts.authinfo` + `accounts.get`/`accounts.list` flow.

The old monolithic `Account` object is split across many v1 sub-resources,
so an account is assembled by fanning out:

accounts/{id}                      core Account
accounts/{id}/homepage             Homepage          (was: websiteUrl)
accounts/{id}/businessInfo         BusinessInfo      (was: businessInformation)
accounts/{id}/businessIdentity     BusinessIdentity
accounts/{id}/automaticImprovements   AutomaticImprovements
accounts/{id}/users (list)         User
accounts/{id}/relationships (list) AccountRelationship
accounts/{id}/services (list)  AccountService
(was: adsLinks / accountManagement)

Output is written in the NATIVE v1 shape (no legacy Content API field names):
one flat JSON record per account, to
``<mc_path>/<account_id>/accounts/rows.jsonlines``.That per-account file
layout keeps the existing BigQuery glob
(``merchant_center/*/accounts/rows.jsonlines``) working unchanged.
"""

from concurrent import futures
import json
import time
from typing import Any, Dict

from absl import logging
from etils import epath
from google.api_core import exceptions as gax_exceptions
from google.shopping import merchant_accounts_v1 as ma

# The Merchant API v1 backend returns sporadic 500 INTERNAL / 503 errors; retry.
_TRANSIENT = (
    gax_exceptions.InternalServerError,
    gax_exceptions.ServiceUnavailable,
    gax_exceptions.DeadlineExceeded,
    gax_exceptions.TooManyRequests,
)

# AccountService.service_type oneof members (each is an
# empty message in the API); we flatten the set oneof to a STRING
# `service_type` for BigQuery.
_SERVICE_TYPE_FIELDS = (
    'products_management',
    'campaigns_management',
    'account_management',
    'account_aggregation',
    'local_listing_management',
    'comparison_shopping',
)

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


def _to_dict(msg) -> Any:
  """Proto -> dict in native v1 shape.

  Args:
    msg: The protobuf message to convert into a dictionary.

  Returns:
    A dictionary representation of the proto message in native v1 shape with
    snake_case keys and string enum names, or None if the input message is
    None.
  """

  if msg is None:
    return None
  return type(msg).to_dict(msg, use_integers_for_enums=False)


class _Clients:
  """Bundle of v1 service clients, all sharing one credential."""

  def __init__(self, credentials):
    self.accounts = ma.AccountsServiceClient(credentials=credentials)
    self.business_info = ma.BusinessInfoServiceClient(
        credentials=credentials)
    self.homepage = ma.HomepageServiceClient(credentials=credentials)
    self.business_identity = ma.BusinessIdentityServiceClient(
        credentials=credentials)
    self.automatic_improvements = ma.AutomaticImprovementsServiceClient(
        credentials=credentials)
    self.users = ma.UserServiceClient(credentials=credentials)
    self.relationships = ma.AccountRelationshipsServiceClient(
        credentials=credentials)
    self.services = ma.AccountServicesServiceClient(
        credentials=credentials)


def _get_or_none(fn, *, what):
  """Single-object getter tolerant of common API error states.

  Args:
    fn: A zero-argument callable (e.g., a lambda) that executes an API call.
    what: A human-readable label string for logging in case of errors.

  Returns:
    A dictionary representation of the fetched object, or None if the request
    results in NOT_FOUND, PERMISSION_DENIED, or FAILED_PRECONDITION.
  """

  try:
    return _to_dict(_retry(fn, what=what))
  except gax_exceptions.NotFound:
    logging.info('%s: NOT_FOUND', what)
    return None
  except gax_exceptions.PermissionDenied as e:
    logging.warning('%s: PERMISSION_DENIED (%s)', what, e.message)
    return None
  except gax_exceptions.FailedPrecondition as e:
    logging.warning('%s: FAILED_PRECONDITION (%s)', what, e.message)
    return None


def _service_type(service_msg) -> str:
  """Returns the set oneof member name (UPPER) for an AccountService.

  Args:
    service_msg: The AccountService protobuf message.

  Returns:
    The upper-case string name of the active 'service_type' oneof field, or
    an empty string if none is set.
  """

  which = type(service_msg).pb(service_msg).WhichOneof('service_type')
  return which.upper() if which else ''


def _build_record(clients: _Clients, account_id: str, core: Dict[str, Any],
                  parent: str, is_advanced: bool) -> Dict[str, Any]:
  """Fans out across v1 sub-resources to build one flat native-shape record.

  Args:
    clients: The `_Clients` object containing initialized API
    client connections.
    account_id: The Merchant Center account ID as a string.
    core: A dictionary containing the core Account information.
    parent: The parent account ID string, or an empty string if top-level.
    is_advanced: Whether the account is an advanced/MCA account.

  Returns:
    A dictionary representing the aggregated native v1 account record,
    including core fields, parent/child relationship metadata, and attached
    sub-resources (homepage, business info, users, etc.).
  """

  name = clients.accounts.account_path(account_id)  # "accounts/{id}"

  services_msgs = _retry(
      lambda: list(
          clients.services.list_account_services(
              ma.ListAccountServicesRequest(parent=name))),
      what=f'list_account_services:{account_id}',
  )
  services = []
  for svc in services_msgs:
    d = _to_dict(svc)
    # Drop the empty oneof messages; expose the set one as `service_type`.
    for k in _SERVICE_TYPE_FIELDS:
      d.pop(k, None)
    d['service_type'] = _service_type(svc)
    services.append(d)

  return {
      # --- core Account (free: already fetched during discovery) ---
      'account_id':
      account_id,
      'account_name':
      core.get('account_name'),
      'adult_content':
      core.get('adult_content'),
      'test_account':
      core.get('test_account'),
      'time_zone':
      core.get('time_zone'),
      'language_code':
      core.get('language_code'),
      # relationship (replaces the legacy {settings, children[]} rollup)
      'is_advanced':
      is_advanced,
      'parent_account':
      parent,
      # sub-resources (each was a field on the old monolithic Account)
      'homepage':
      _get_or_none(
          lambda: clients.homepage.get_homepage(
              name=clients.homepage.homepage_path(account_id)),
          what=f'homepage:{account_id}'),
      'business_info':
      _get_or_none(
          lambda: clients.business_info.get_business_info(
              name=clients.business_info.business_info_path(account_id)),
          what=f'business_info:{account_id}'),
      'business_identity':
      _get_or_none(
          lambda: clients.business_identity.get_business_identity(
              name=clients.business_identity.business_identity_path(
                  account_id)),
          what=f'business_identity:{account_id}'),
      'automatic_improvements':
      _get_or_none(
          lambda: clients.automatic_improvements.get_automatic_improvements(
              name=clients.automatic_improvements.
              automatic_improvements_path(account_id)),
          what=f'automatic_improvements:{account_id}'),
      'users': [
          _to_dict(u) for u in _retry(
              lambda: list(
                  clients.users.list_users(
                      ma.ListUsersRequest(parent=name))),
              what=f'list_users:{account_id}')
      ],
      'account_relationships': [
          _to_dict(r) for r in _retry(
              lambda: list(
                  clients.relationships.list_account_relationships(
                      ma.ListAccountRelationshipsRequest(parent=name))),
              what=f'list_account_relationships:{account_id}')
      ],
      'account_services':
      services,
  }


def discover_topology(clients: _Clients, input_ids):
  """Classifies input accounts and enumerates sub-accounts.

  Replaces authinfo by organizing accounts into top-level aggregators,
  standalone accounts, and child accounts.

  Args:
    clients: The `_Clients` object containing initialized API client
      connections.
    input_ids: An iterable of account IDs (e.g., strings or integers) to
      process.

  Returns:
    A tuple containing:
      - aggregator_ids: set of advanced (MCA) account IDs from the input.
      - standalone_ids: set of standalone (non-advanced) input account IDs.
      - leaf_to_parent: {child_id: aggregator_id} for every sub-account.
      - cores: {account_id: core Account dict} (sub-account cores come free
        from the listSubaccounts response; only top-level/advanced accounts
        cost a get).
  """
  aggregator_ids, standalone_ids = set(), set()
  leaf_to_parent = {}
  cores = {}
  for acc_id in input_ids:
    provider = clients.accounts.account_path(acc_id)
    try:
      subs = _retry(
          lambda p=provider: list(
              clients.accounts.list_sub_accounts(
                  ma.ListSubAccountsRequest(provider=p)
              )
          ),
          what=f'list_sub_accounts:{acc_id}',
      )
    except gax_exceptions.PermissionDenied as e:
      logging.warning(
          'No access to account %s (%s); skipping.', acc_id, e.message
      )
      continue
    if subs:
      logging.info(
          'Account %s is advanced/MCA with %d sub-accounts',
          acc_id,
          len(subs),
      )
      aggregator_ids.add(acc_id)
      cores[acc_id] = _to_dict(
          _retry(
              lambda p=provider: clients.accounts.get_account(name=p),
              what=f'get_account:{acc_id}',
          )
      )
      for s in subs:
        sid = str(s.account_id)
        cores[sid] = _to_dict(
            s
        )  # core from list response -- 0 extra calls
        leaf_to_parent[sid] = acc_id
    else:
      logging.info('Account %s is standalone', acc_id)
      standalone_ids.add(acc_id)
      cores[acc_id] = _to_dict(
          _retry(
              lambda p=provider: clients.accounts.get_account(name=p),
              what=f'get_account:{acc_id}',
          )
      )
  return aggregator_ids, standalone_ids, leaf_to_parent, cores


def download_accounts(credentials,
                      input_ids,
                      mc_path,
                      max_workers=_MAX_WORKERS):
  """Downloads accounts from Merchant API v1 and writes flat files.

  Args:
    credentials: Google credentials (same as used for the Ads/Content APIs).
    input_ids: iterable of top-level Merchant Center account IDs.
    mc_path: epath.Path to the merchant_center output directory.
    max_workers: max concurrent account downloads. Callers (namely `acit.py`)
      should pass this explicitly so it stays consistent with the other
      Merchant API ingestion stages.

  Returns:
    (aggregator_ids, standalone_ids, leaf_to_parent) topology for reuse by the
    still-on-Content-API resource pulls (products, liasettings, etc.).
  """
  clients = _Clients(credentials)
  input_ids = list(input_ids)
  aggregator_ids, standalone_ids, leaf_to_parent, cores = discover_topology(
      clients, input_ids)

  def _process(account_id):
    parent = leaf_to_parent.get(account_id)
    is_advanced = account_id in aggregator_ids
    record = _build_record(clients, account_id, cores[account_id], parent,
                           is_advanced)
    output_file = (epath.Path(mc_path) / account_id / 'accounts' /
                   'rows.jsonlines')
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open('w') as f:
      f.write(json.dumps(record) + '\n')
    return account_id

  with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
    future_to_id = {ex.submit(_process, aid): aid for aid in cores}
    for done in futures.as_completed(future_to_id):
      aid = future_to_id[done]
      done.result()  # surface exceptions
      logging.info('Wrote account record for %s', aid)

  logging.info(
      'Merchant API accounts: %d advanced, %d standalone, %d sub-accounts',
      len(aggregator_ids), len(standalone_ids), len(leaf_to_parent))
  return aggregator_ids, standalone_ids, leaf_to_parent
