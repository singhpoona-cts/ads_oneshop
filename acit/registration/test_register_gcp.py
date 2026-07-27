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
"""Unit tests for the GCP Merchant Center registration utility."""

import json
import os
from unittest import mock

from absl.testing import absltest
from absl.testing import flagsaver
from acit.registration import register_gcp
from etils import epath
from google.api_core import exceptions as gax_exceptions
from google.auth import credentials
from google.shopping import merchant_accounts_v1


class RegisterGcpTest(absltest.TestCase):
  """Tests for the GCP Merchant Center registration utility tool."""

  def setUp(self):
    """Saves the current environment variables before tests run."""

    super().setUp()
    # Save environment variables to restore them after tests
    self.original_env = dict(os.environ)

  def tearDown(self):
    """Restores the environment variables to their original state."""

    os.environ.clear()
    os.environ.update(self.original_env)
    super().tearDown()

  def test_get_credentials_from_env(self):
    """Verifies credentials can be loaded directly from environment variables."""

    os.environ['GOOGLE_ADS_REFRESH_TOKEN'] = 'env-refresh-token'
    os.environ['GOOGLE_ADS_CLIENT_ID'] = 'env-client-id'
    os.environ['GOOGLE_ADS_CLIENT_SECRET'] = 'env-client-secret'

    # Unused/non-existent paths
    secrets_path = epath.Path('/does/not/exist/secrets.json')
    refresh_path = epath.Path('/does/not/exist/refresh.txt')

    creds = register_gcp.get_credentials(secrets_path, refresh_path)
    self.assertEqual(creds.refresh_token, 'env-refresh-token')
    self.assertEqual(creds.client_id, 'env-client-id')
    self.assertEqual(creds.client_secret, 'env-client-secret')

  def test_get_credentials_from_files(self):
    """Verifies credentials can be loaded from local configuration files."""

    # Ensure env vars are not set
    os.environ.pop('GOOGLE_ADS_REFRESH_TOKEN', None)
    os.environ.pop('GOOGLE_ADS_CLIENT_ID', None)
    os.environ.pop('GOOGLE_ADS_CLIENT_SECRET', None)

    secrets_content = {
        'web': {
            'client_id': 'file-client-id',
            'client_secret': 'file-client-secret',
        }
    }
    secrets_file = self.create_tempfile(content=json.dumps(secrets_content))
    refresh_file = self.create_tempfile(content='file-refresh-token')

    creds = register_gcp.get_credentials(
        epath.Path(secrets_file), epath.Path(refresh_file)
    )
    self.assertEqual(creds.refresh_token, 'file-refresh-token')
    self.assertEqual(creds.client_id, 'file-client-id')
    self.assertEqual(creds.client_secret, 'file-client-secret')

  @mock.patch('google.auth.default')
  def test_get_credentials_fallback_to_adc(self, mock_auth_default):
    """Verifies fallback to Application Default Credentials when all else fails."""

    # Ensure env vars are not set
    os.environ.pop('GOOGLE_ADS_REFRESH_TOKEN', None)
    os.environ.pop('GOOGLE_ADS_CLIENT_ID', None)
    os.environ.pop('GOOGLE_ADS_CLIENT_SECRET', None)

    mock_creds = mock.create_autospec(credentials.Credentials, instance=True)
    mock_auth_default.return_value = (mock_creds, 'project-id')

    # Unused/non-existent paths
    secrets_path = epath.Path('/does/not/exist/secrets.json')
    refresh_path = epath.Path('/does/not/exist/refresh.txt')

    creds = register_gcp.get_credentials(secrets_path, refresh_path)
    self.assertEqual(creds, mock_creds)
    mock_auth_default.assert_called_once()

  @mock.patch.object(merchant_accounts_v1.DeveloperRegistrationServiceClient,
                     'register_gcp')
  def test_register_gcp_project_success(self, mock_register_gcp):
    """Verifies the GCP project is registered correctly via the API client."""

    mock_creds = mock.create_autospec(credentials.Credentials, instance=True)
    account_id = '123456789'
    developer_email = 'admin@example.com'

    # Mock the return value of register_gcp call
    expected_response = mock.Mock()
    mock_register_gcp.return_value = expected_response

    register_gcp.register_gcp_project(account_id, developer_email, mock_creds)

    expected_request = merchant_accounts_v1.RegisterGcpRequest(
        name=f'accounts/{account_id}/developerRegistration',
        developer_email=developer_email,
    )
    mock_register_gcp.assert_called_once_with(request=expected_request)

  @mock.patch.object(merchant_accounts_v1.DeveloperRegistrationServiceClient,
                     'get_account_for_gcp_registration')
  @mock.patch.object(merchant_accounts_v1.DeveloperRegistrationServiceClient,
                     'register_gcp')
  def test_register_gcp_project_already_exists(
      self, mock_register_gcp, mock_get_existing
  ):
    """Verifies handling of already registered projects."""

    mock_creds = mock.create_autospec(credentials.Credentials, instance=True)
    account_id = '123456789'
    developer_email = 'admin@example.com'

    # Mock AlreadyExists exception
    mock_register_gcp.side_effect = gax_exceptions.AlreadyExists('Duplicate')

    # Mock the return value of get_account_for_gcp_registration
    expected_existing = mock.Mock()
    expected_existing.name = f'accounts/{account_id}'
    mock_get_existing.return_value = expected_existing

    register_gcp.register_gcp_project(account_id, developer_email, mock_creds)

    mock_register_gcp.assert_called_once()
    mock_get_existing.assert_called_once_with(request={})

  def test_main_missing_account_id(self):
    """Verifies the main function fails when account_id flag is missing."""

    with flagsaver.as_parsed(account_id='', developer_email='test@test.com'):
      with self.assertRaisesRegex(
          register_gcp.app.UsageError, '--account_id is required.'):
          register_gcp.main([])

  def test_main_missing_developer_email(self):
    """Verifies the main function fails when developer_email flag is missing."""

    with flagsaver.as_parsed(account_id='123456789', developer_email=''):
      with self.assertRaisesRegex(register_gcp.app.UsageError,
                                  '--developer_email is required.'):
        register_gcp.main([])

  @mock.patch.object(register_gcp, 'get_credentials')
  @mock.patch.object(register_gcp, 'register_gcp_project')
  def test_main_success(self, mock_register_gcp_project, mock_get_credentials):
    """Verifies the main function correctly orchestrates the registration."""

    with flagsaver.as_parsed(account_id='123456789',
                             developer_email='admin@example.com'):
      mock_creds = mock.create_autospec(credentials.Credentials, instance=True)
      mock_get_credentials.return_value = mock_creds

      register_gcp.main([])

      mock_get_credentials.assert_called_once()
      mock_register_gcp_project.assert_called_once_with(
          account_id='123456789',
          developer_email='admin@example.com',
          creds=mock_creds,
      )


if __name__ == '__main__':
  absltest.main()
