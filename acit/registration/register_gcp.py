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
"""Utility script to register a Google Cloud project with a Merchant Center account.

This corresponds to the one-time developer registration setup via the
`registerGcp` call of the DeveloperRegistrationService in the Merchant API.
"""

import os
from collections.abc import Sequence
from absl import app
from absl import flags
from absl import logging
from acit.auth import oauth
from etils import epath
from google import auth
from google.auth import credentials
from google.oauth2 import credentials as oauth_credentials
from google.shopping.merchant_accounts_v1 import (
    DeveloperRegistrationServiceClient,
    RegisterGcpRequest,
)

_OAUTH_TOKEN_ENDPOINT = 'https://oauth2.googleapis.com/token'

_ACCOUNT_ID = flags.DEFINE_string(
    'account_id',
    None,
    'The Merchant Center account ID.',
)

_DEVELOPER_EMAIL = flags.DEFINE_string(
    'developer_email',
    None,
    'The Merchant Center admin developer email.',
)

_CLIENT_SECRETS_PATH = epath.DEFINE_path(
    'client_secrets_path',
    'client_secrets.json',
    'The path to the client secrets file.',
)

_REFRESH_TOKEN_PATH = epath.DEFINE_path(
    'refresh_token_path',
    'refresh_token.txt',
    'The path to the OAuth refresh token file.',
)


def get_credentials(
    client_secrets_path: epath.Path,
    refresh_token_path: epath.Path,
) -> credentials.Credentials:
  """Get credentials using env vars, config files, or App Default Credentials."""
  refresh_token = os.environ.get('GOOGLE_ADS_REFRESH_TOKEN', '').strip()
  client_id = os.environ.get('GOOGLE_ADS_CLIENT_ID', '').strip()
  client_secret = os.environ.get('GOOGLE_ADS_CLIENT_SECRET', '').strip()

  if not (refresh_token and client_id and client_secret):
    # Try to load from local files if they exist
    if client_secrets_path.exists() and refresh_token_path.exists():
      logging.info('Loading OAuth credentials from local files.')
      try:
        secrets = oauth.get_secrets_dict(str(client_secrets_path))
        client_id, client_secret = oauth.get_client_id_and_secret(secrets)
        refresh_token = refresh_token_path.read_text().strip()
      except Exception as e:
        logging.warning('Failed to load OAuth credentials from files: %s', e)

  if refresh_token and client_id and client_secret:
    logging.info('Using OAuth credentials for registration.')
    return oauth_credentials.Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri=_OAUTH_TOKEN_ENDPOINT,
        client_id=client_id,
        client_secret=client_secret,
    )

  logging.info('Falling back to Application Default Credentials.')
  creds, _ = auth.default()
  assert isinstance(creds, credentials.Credentials)
  return creds


def register_gcp_project(
    account_id: str,
    developer_email: str,
    creds: credentials.Credentials,
) -> None:
  """Registers the Google Cloud project with the Merchant Center account."""
  logging.info(
      'Registering GCP project with Merchant Center account %s for developer %s',
      account_id,
      developer_email,
  )
  client = DeveloperRegistrationServiceClient(credentials=creds)
  request = RegisterGcpRequest(
      name=f'accounts/{account_id}/developerRegistration',
      developer_email=developer_email,
  )
  response = client.register_gcp(request=request)
  logging.info('Successfully registered GCP project. Response: %s', response)
  print(f'Registration successful! Response Details:\n{response}')


def main(argv: Sequence[str]) -> None:
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  if not _ACCOUNT_ID.value:
    raise app.UsageError('--account_id is required.')
  if not _DEVELOPER_EMAIL.value:
    raise app.UsageError('--developer_email is required.')

  creds = get_credentials(
      client_secrets_path=_CLIENT_SECRETS_PATH.value,
      refresh_token_path=_REFRESH_TOKEN_PATH.value,
  )

  try:
    register_gcp_project(
        account_id=_ACCOUNT_ID.value,
        developer_email=_DEVELOPER_EMAIL.value,
        creds=creds,
    )
  except Exception as e:
    logging.error('Error during GCP registration: %s', e)
    raise e


if __name__ == '__main__':
  app.run(main)
