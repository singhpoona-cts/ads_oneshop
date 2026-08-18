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

This replaces the old Content API
`accounts.authinfo` + `accounts.get`/`accounts.list` flow
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

# The row shape is defined once, as ``acit.api.v0.storage.Account`` in
# ``schema.proto``, which also generates the BigQuery schema. This module
# constructs the strongly typed ``Account`` protobuf messages and emits them as
# JSON for BigQuery ingestion and Beam processing.
"""

from concurrent import futures
import json

from absl import logging
from acit.api.v0.storage import schema_pb2
from etils import epath
from google.api_core import exceptions as gax_exceptions
from google.protobuf import json_format
from google.shopping import merchant_accounts_v1 as ma

# AccountService.service_type oneof members (each is an
# empty message in the API); we flatten the set oneof to a STRING
# `service_type` for BigQuery. See `AccountServiceRow` in schema.proto.
_SERVICE_TYPE_FIELDS = (
    'products_management',
    'campaigns_management',
    'account_management',
    'account_aggregation',
    'local_listing_management',
    'comparison_shopping',
)


def _to_service_row(
    service_msg: ma.AccountService,
) -> schema_pb2.AccountServiceRow:
  """Converts one v1 AccountService into an AccountServiceRow proto message.

  Args:
    service_msg: The AccountService protobuf message.

  Returns:
    The schema_pb2.AccountServiceRow proto message with the `service_type` oneof
    flattened to a string.
  """
  row = schema_pb2.AccountServiceRow()
  pb_msg = ma.AccountService.pb(service_msg)
  if pb_msg.name:
    row.name = pb_msg.name
  if pb_msg.provider:
    row.provider = pb_msg.provider
  if pb_msg.provider_display_name:
    row.provider_display_name = pb_msg.provider_display_name
  if pb_msg.external_account_id:
    row.external_account_id = pb_msg.external_account_id
  if pb_msg.mutability:
    row.mutability = pb_msg.mutability
  row.service_type = _service_type_name(service_msg)
  if pb_msg.HasField('handshake'):
    row.handshake.CopyFrom(pb_msg.handshake)
  return row


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
    self.programs = ma.ProgramsServiceClient(credentials=credentials)


def _get_or_none(request_fn, *, resource_label):
  """Single-object getter tolerant of common API error states.

  Args:
    request_fn: A zero-argument callable (e.g., a lambda) that executes an API
      call.
    resource_label: A human-readable label for logging in case of errors.

  Returns:
    A protobuf message, or None if the request
    results in NOT_FOUND, PERMISSION_DENIED, or FAILED_PRECONDITION.
  """

  try:
    return request_fn()
  except gax_exceptions.NotFound:
    logging.info('%s: NOT_FOUND', resource_label)
    return None
  except gax_exceptions.PermissionDenied as e:
    logging.warning('%s: PERMISSION_DENIED (%s)', resource_label, e.message)
    return None
  except gax_exceptions.FailedPrecondition as e:
    logging.warning('%s: FAILED_PRECONDITION (%s)', resource_label, e.message)
    return None


def _service_type_name(service_msg) -> str:
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


def _fetch_account_row(
    clients: _Clients,
    account_id: str,
    base_account: ma.Account,
    parent: str | None,
    is_advanced: bool,
) -> schema_pb2.Account:
  """Fans out across the v1 sub-resources for one account and flattens them.

  Issues one request per sub-resource (7 in total), so this is I/O-bound; it is
  called once per account from a thread pool.

  Args:
    clients: The `_Clients` object containing initialized API client
      connections.
    account_id: The Merchant Center account ID as a string.
    base_account: An Account protobuf message containing base Account
      information.
    parent: The parent (aggregator) account ID, or None if this account is
      standalone or is itself an aggregator.
    is_advanced: Whether the account is an advanced/MCA account.

  Returns:
    One `schema_pb2.Account` protobuf message.
  """

  # "accounts/{id}". The v1 list methods take this as their `parent`.
  account_resource_name = clients.accounts.account_path(account_id)

  services_msgs = list(
      clients.services.list_account_services(
          ma.ListAccountServicesRequest(parent=account_resource_name)))

  homepage = _get_or_none(
      lambda: clients.homepage.get_homepage(
          name=clients.homepage.homepage_path(account_id)),
      resource_label=f'homepage:{account_id}')
  business_info = _get_or_none(
      lambda: clients.business_info.get_business_info(
          name=clients.business_info.business_info_path(account_id)),
      resource_label=f'business_info:{account_id}')
  business_identity = _get_or_none(
      lambda: clients.business_identity.get_business_identity(
          name=clients.business_identity.business_identity_path(account_id)),
      resource_label=f'business_identity:{account_id}')
  automatic_improvements = _get_or_none(
      lambda: clients.automatic_improvements.get_automatic_improvements(
          name=clients.automatic_improvements.automatic_improvements_path(
              account_id)),
      resource_label=f'automatic_improvements:{account_id}')

  account_msg = schema_pb2.Account()
  account_msg.account_id = int(account_id)
  if base_account:
    if base_account.account_name:
      account_msg.account_name = base_account.account_name
    account_msg.adult_content = base_account.adult_content
    account_msg.test_account = base_account.test_account
    if base_account.time_zone:
      account_msg.time_zone.CopyFrom(ma.Account.pb(base_account).time_zone)
    if base_account.language_code:
      account_msg.language_code = base_account.language_code
  account_msg.is_advanced = is_advanced
  if parent is not None:
    account_msg.parent_account = int(parent)

  if homepage:
    account_msg.homepage.CopyFrom(ma.Homepage.pb(homepage))
  if business_info:
    account_msg.business_info.CopyFrom(ma.BusinessInfo.pb(business_info))
  if business_identity:
    account_msg.business_identity.CopyFrom(
        ma.BusinessIdentity.pb(business_identity))
  if automatic_improvements:
    account_msg.automatic_improvements.CopyFrom(
        ma.AutomaticImprovements.pb(automatic_improvements))

  for u in clients.users.list_users(
      ma.ListUsersRequest(parent=account_resource_name)):
    account_msg.users.add().CopyFrom(ma.User.pb(u))

  for r in clients.relationships.list_account_relationships(
      ma.ListAccountRelationshipsRequest(
          parent=account_resource_name)):
    account_msg.account_relationships.add().CopyFrom(
        ma.AccountRelationship.pb(r))

  for s in services_msgs:
    account_msg.account_services.append(_to_service_row(s))

  for p in clients.programs.list_programs(
      ma.ListProgramsRequest(parent=account_resource_name)):
    account_msg.programs.add().CopyFrom(ma.Program.pb(p))

  return account_msg


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
      - base_accounts_by_id: {account_id: base Account message} (sub-account
        base messages come free from the listSubaccounts response; only
        top-level/advanced accounts cost a get).
  """
  aggregator_ids, standalone_ids = set(), set()
  leaf_to_parent = {}
  base_accounts_by_id = {}
  for acc_id in input_ids:
    provider = clients.accounts.account_path(acc_id)
    try:
      base_account = clients.accounts.get_account(name=provider)
    except (gax_exceptions.PermissionDenied, gax_exceptions.NotFound) as e:
      logging.warning(
          'No access to base account info for %s (%s); '
          'checking if it is an advanced account.',
          acc_id,
          e.message,
      )
      base_account = None

    subs = []
    try:
      subs = list(
          clients.accounts.list_sub_accounts(
              ma.ListSubAccountsRequest(provider=provider)
          )
      )
    except (gax_exceptions.PermissionDenied,
            gax_exceptions.FailedPrecondition) as e:
      if base_account is None:
        logging.warning(
            'Truly no access to account %s (%s); skipping.',
            acc_id,
            e.message,
        )
        continue
      logging.info(
          'Account %s is not an advanced account (%s); treating as standalone.',
          acc_id,
          e.message,
      )

    if subs:
      logging.info(
          'Account %s is advanced/MCA with %d sub-accounts',
          acc_id,
          len(subs),
      )
      aggregator_ids.add(acc_id)
      base_accounts_by_id[acc_id] = base_account
      for s in subs:
        sid = str(s.account_id)
        base_accounts_by_id[sid] = s
        leaf_to_parent[sid] = acc_id
    else:
      if base_account is None:
        logging.warning(
            'Account %s has no sub-accounts and no base info; skipping.',
            acc_id,
        )
        continue
      logging.info('Account %s is standalone', acc_id)
      standalone_ids.add(acc_id)
      base_accounts_by_id[acc_id] = base_account
  return aggregator_ids, standalone_ids, leaf_to_parent, base_accounts_by_id


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
  aggregator_ids, standalone_ids, leaf_to_parent, base_accounts_by_id = (
      discover_topology(clients, input_ids)
  )

  def _process(account_id):
    parent = leaf_to_parent.get(account_id)
    is_advanced = account_id in aggregator_ids
    record = _fetch_account_row(
        clients,
        account_id,
        base_accounts_by_id[account_id],
        parent,
        is_advanced,
    )
    output_file = (epath.Path(mc_path) / account_id / 'accounts' /
                   'rows.jsonlines')
    output_file.parent.mkdir(parents=True, exist_ok=True)
    record_dict = json_format.MessageToDict(
        record,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )
    with output_file.open('w') as f:
      f.write(json.dumps(record_dict) + '\n')
    return account_id

  with futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
    future_to_id = {
        ex.submit(_process, aid): aid for aid in base_accounts_by_id
    }
    for done in futures.as_completed(future_to_id):
      aid = future_to_id[done]
      done.result()  # surface exceptions
      logging.info('Wrote account record for %s', aid)

  logging.info(
      'Merchant API accounts: %d advanced, %d standalone, %d sub-accounts',
      len(aggregator_ids), len(standalone_ids), len(leaf_to_parent))
  return aggregator_ids, standalone_ids, leaf_to_parent
