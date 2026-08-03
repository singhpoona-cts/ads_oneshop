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

This replaces the old Content API `accounts.authinfo` + `accounts.get`/`accounts.list` flow
as part of the Content API -> Merchant API migration.

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
import dataclasses
import json
from typing import Any, Dict

from absl import logging
from acit.utils import to_dict as _to_dict
from etils import epath
from google.api_core import exceptions as gax_exceptions
from google.protobuf import json_format
from google.shopping import merchant_accounts_v1 as ma

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


@dataclasses.dataclass
class AccountRecord:
  """Strongly-typed wrapper representing an aggregated Merchant account."""
  account_id: str
  account_name: str | None
  adult_content: bool | None
  test_account: bool | None
  time_zone: Any
  language_code: str | None
  is_advanced: bool
  parent_account: str | None
  homepage: ma.Homepage | None
  business_info: ma.BusinessInfo | None
  business_identity: ma.BusinessIdentity | None
  automatic_improvements: ma.AutomaticImprovements | None
  users: list[ma.User]
  account_relationships: list[ma.AccountRelationship]
  account_services: list[ma.AccountService]


def _serialize_record(record: AccountRecord) -> Dict[str, Any]:
  """Serializes a strongly-typed AccountRecord into the exact BigQuery schema dict."""
  homepage_dict = _to_dict(record.homepage) if record.homepage else None
  business_info_dict = _to_dict(record.business_info) if record.business_info else None
  business_identity_dict = _to_dict(record.business_identity) if record.business_identity else None
  automatic_improvements_dict = _to_dict(record.automatic_improvements) if record.automatic_improvements else None
  time_zone_dict = _to_dict(record.time_zone) if record.time_zone else None

  users_dicts = [_to_dict(u) for u in record.users]
  relationships_dicts = [_to_dict(r) for r in record.account_relationships]

  services_dicts = []
  for svc in record.account_services:
    d = _to_dict(svc)
    # Drop the empty oneof messages; expose the set one as `service_type`.
    for k in _SERVICE_TYPE_FIELDS:
      d.pop(k, None)
    d['service_type'] = _service_type(svc)
    services_dicts.append(d)

  return {
      'account_id': record.account_id,
      'account_name': record.account_name,
      'adult_content': record.adult_content,
      'test_account': record.test_account,
      'time_zone': time_zone_dict,
      'language_code': record.language_code,
      'is_advanced': record.is_advanced,
      'parent_account': record.parent_account,
      'homepage': homepage_dict,
      'business_info': business_info_dict,
      'business_identity': business_identity_dict,
      'automatic_improvements': automatic_improvements_dict,
      'users': users_dicts,
      'account_relationships': relationships_dicts,
      'account_services': services_dicts,
  }


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
    A protobuf message, or None if the request
    results in NOT_FOUND, PERMISSION_DENIED, or FAILED_PRECONDITION.
  """

  try:
    return fn()
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

  if service_msg is None:
    return ''
  which = type(service_msg).pb(service_msg).WhichOneof('service_type')
  return which.upper() if which else ''


def _build_record(clients: _Clients, account_id: str, core: ma.Account,
                  parent: str | None, is_advanced: bool) -> AccountRecord:
  """Fans out across v1 sub-resources to build one flat native-shape record.

  Args:
    clients: The `_Clients` object containing initialized API
    client connections.
    account_id: The Merchant Center account ID as a string.
    core: An Account protobuf message containing core Account information.
    parent: The parent account ID string, or an empty string if top-level.
    is_advanced: Whether the account is an advanced/MCA account.

  Returns:
    An AccountRecord dataclass representing the aggregated native v1 account.
  """

  name = clients.accounts.account_path(account_id)  # "accounts/{id}"

  services_msgs = list(
      clients.services.list_account_services(
          ma.ListAccountServicesRequest(parent=name)))

  return AccountRecord(
      account_id=account_id,
      account_name=core.account_name,
      adult_content=core.adult_content,
      test_account=core.test_account,
      time_zone=core.time_zone,
      language_code=core.language_code,
      is_advanced=is_advanced,
      parent_account=parent,
      homepage=_get_or_none(
          lambda: clients.homepage.get_homepage(
              name=clients.homepage.homepage_path(account_id)),
          what=f'homepage:{account_id}'),
      business_info=_get_or_none(
          lambda: clients.business_info.get_business_info(
              name=clients.business_info.business_info_path(account_id)),
          what=f'business_info:{account_id}'),
      business_identity=_get_or_none(
          lambda: clients.business_identity.get_business_identity(
              name=clients.business_identity.business_identity_path(
                  account_id)),
          what=f'business_identity:{account_id}'),
      automatic_improvements=_get_or_none(
          lambda: clients.automatic_improvements.get_automatic_improvements(
              name=clients.automatic_improvements.
              automatic_improvements_path(account_id)),
          what=f'automatic_improvements:{account_id}'),
      users=list(
          clients.users.list_users(
              ma.ListUsersRequest(parent=name))),
      account_relationships=list(
          clients.relationships.list_account_relationships(
              ma.ListAccountRelationshipsRequest(parent=name))),
      account_services=services_msgs,
  )


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
      - cores: {account_id: core Account message} (sub-account cores come free
        from the listSubaccounts response; only top-level/advanced accounts
        cost a get).
  """
  aggregator_ids, standalone_ids = set(), set()
  leaf_to_parent = {}
  cores = {}
  for acc_id in input_ids:
    provider = clients.accounts.account_path(acc_id)
    try:
      subs = list(
          clients.accounts.list_sub_accounts(
              ma.ListSubAccountsRequest(provider=provider)
          )
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
      cores[acc_id] = clients.accounts.get_account(name=provider)
      for s in subs:
        sid = str(s.account_id)
        cores[sid] = s  # core from list response -- 0 extra calls
        leaf_to_parent[sid] = acc_id
    else:
      logging.info('Account %s is standalone', acc_id)
      standalone_ids.add(acc_id)
      cores[acc_id] = clients.accounts.get_account(name=provider)
  return aggregator_ids, standalone_ids, leaf_to_parent, cores


def download_accounts(credentials,
                      input_ids,
                      mc_path,
                      max_workers=None):
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
    serialized = _serialize_record(record)
    output_file = (epath.Path(mc_path) / account_id / 'accounts' /
                   'rows.jsonlines')
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open('w') as f:
      f.write(json.dumps(serialized) + '\n')
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
