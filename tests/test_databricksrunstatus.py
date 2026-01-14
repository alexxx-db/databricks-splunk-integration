import declare
import unittest
import sys
import os
from mock import patch, MagicMock, call

mocked_modules = {}


def setUpModule():
    global mocked_modules

    module_to_be_mocked = [
        'log_manager',
        'splunk',
        'splunk.rest',
        'splunk.admin',
        'splunk.clilib',
        'splunk.clilib.cli_common',
        'solnlib',
        'solnlib.server_info',
        'solnlib.utils',
        'solnlib.credentials',
        'splunk_aoblib',
        'splunk_aoblib.rest_migration',
        'splunklib',
        'splunklib.client',
        'splunklib.results',
        'splunklib.binding',
        'splunk.Intersplunk',
        'splunktaucclib',
        'splunktaucclib.rest_handler',
        'splunktaucclib.rest_handler.endpoint',
        'splunktaucclib.rest_handler.endpoint.validator'
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}

    for module, magicmock in mocked_modules.items():
        patch.dict('sys.modules', **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()


class TestDatabricksrunstatus(unittest.TestCase):
    """Test databricksrunstatus script."""

    def _run_script(self):
        """Helper method to run the databricksrunstatus script as __main__."""
        # Delete the module from cache to force reimport
        if 'databricksrunstatus' in sys.modules:
            del sys.modules['databricksrunstatus']

        try:
            # Read and execute the script with __name__ == "__main__"
            script_path = os.path.join(
                os.path.dirname(__file__),
                '..',
                'app',
                'bin',
                'databricksrunstatus.py'
            )
            script_path = os.path.abspath(script_path)

            with open(script_path, 'r') as f:
                script_code = f.read()

            # Create a globals dict with __name__ set to "__main__"
            script_globals = {
                '__name__': '__main__',
                '__file__': script_path,
            }

            # Execute the script code
            exec(compile(script_code, script_path, 'exec'), script_globals)
        except SystemExit:
            # Script calls sys.exit, which we want to capture
            pass

    def test_no_results_early_exit(self):
        """Test early exit when no results are returned (lines 24-26)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('sys.exit') as mock_exit:

            # Mock getOrganizedResults to return empty results
            mock_get_results.return_value = ([], {}, {'sessionKey': 'test_session_key'})

            # Execute the script
            self._run_script()

            # Verify sys.exit(0) was called
            mock_exit.assert_called_once_with(0)

    def test_running_state_no_status_change(self):
        """Test RUNNING state when status hasn't changed (lines 49-53)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '123',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',  # Already Running
                'uid': 'test_uid'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'RUNNING'
                }
            }

            # Execute the script
            self._run_script()

            # Verify no ingestion occurred since status didn't change
            mock_ingest.assert_not_called()

    def test_state_change_to_running(self):
        """Test state change to RUNNING triggers ingestion (lines 49-53)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '123',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Pending',  # Different from RUNNING
                'uid': 'test_uid'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'RUNNING'
                }
            }

            # Execute the script
            self._run_script()

            # Verify ingestion occurred
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertEqual(ingested_data['run_execution_status'], 'Running')
            self.assertEqual(ingested_data['created_time'], 1234567890.0)

    def test_pending_state_transition(self):
        """Test PENDING state transition (lines 55-59)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '456',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',  # Different from PENDING
                'uid': 'test_uid_456'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'PENDING'
                }
            }

            # Execute the script
            self._run_script()

            # Verify ingestion occurred with Pending status
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertEqual(ingested_data['run_execution_status'], 'Pending')
            self.assertEqual(ingested_data['created_time'], 1234567890.0)

    def test_terminated_success_state(self):
        """Test TERMINATED/SUCCESS state (lines 61-65)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '789',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',
                'uid': 'test_uid_789'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'TERMINATED',
                    'result_state': 'SUCCESS'
                }
            }

            # Execute the script
            self._run_script()

            # Verify ingestion occurred with Success status
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertEqual(ingested_data['run_execution_status'], 'Success')
            self.assertEqual(ingested_data['created_time'], 1234567890.0)

    def test_terminated_failed_state(self):
        """Test TERMINATED/FAILED state (lines 66-69)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '999',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',
                'uid': 'test_uid_999'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'TERMINATED',
                    'result_state': 'FAILED'
                }
            }

            # Execute the script
            self._run_script()

            # Verify ingestion occurred with Failed status
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertEqual(ingested_data['run_execution_status'], 'Failed')

    def test_terminated_canceled_state(self):
        """Test TERMINATED/CANCELED state (lines 70-73)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '111',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',
                'uid': 'test_uid_111'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'TERMINATED',
                    'result_state': 'CANCELED'
                }
            }

            # Execute the script
            self._run_script()

            # Verify ingestion occurred with Canceled status
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertEqual(ingested_data['run_execution_status'], 'Canceled')

    def test_other_state_handling(self):
        """Test other state handling (lines 74-80)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '222',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',
                'uid': 'test_uid_222'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'UNKNOWN_STATE',
                    'result_state': 'UNKNOWN_RESULT'
                }
            }

            # Execute the script
            self._run_script()

            # Verify ingestion occurred with the new state
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertEqual(ingested_data['run_execution_status'], 'UNKNOWN_RESULT')

    def test_invalid_run_id_value_error(self):
        """Test invalid run_id handling (lines 44-46, ValueError)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('sys.exit'):

            # Setup test data with invalid run_id
            test_result = {
                'run_id': 'invalid_run_id',  # String that can't be converted to int
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',
                'uid': 'test_uid_invalid'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient (should not be called)
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client

            # Execute the script
            self._run_script()

            # Verify no ingestion occurred and databricks_api was not called
            mock_ingest.assert_not_called()
            mock_client.databricks_api.assert_not_called()

    def test_exception_handling_per_result(self):
        """Test exception handling per result continues processing (lines 87-89)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data with two results
            test_result1 = {
                'run_id': '333',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',
                'uid': 'test_uid_333'
            }
            test_result2 = {
                'run_id': '444',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',
                'uid': 'test_uid_444'
            }
            mock_get_results.return_value = (
                [test_result1, test_result2],
                {},
                {'sessionKey': 'test_session_key'}
            )

            # Mock DatabricksClient - first call raises exception, second succeeds
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.side_effect = [
                Exception("API error"),  # First result fails
                {  # Second result succeeds
                    'state': {
                        'life_cycle_state': 'TERMINATED',
                        'result_state': 'SUCCESS'
                    }
                }
            ]

            # Execute the script
            self._run_script()

            # Verify that second result was still processed despite first failing
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertEqual(ingested_data['run_id'], '444')
            self.assertEqual(ingested_data['run_execution_status'], 'Success')

    def test_outer_exception_handling(self):
        """Test outer exception handling (lines 91-94)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('sys.exit') as mock_exit:

            # Mock getOrganizedResults to raise an exception
            mock_get_results.side_effect = Exception("Fatal error")

            # Execute the script
            self._run_script()

            # Verify sys.exit(0) was called
            mock_exit.assert_called_once_with(0)

    def test_param_field_splitting(self):
        """Test param field is split on newlines (line 32)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data with param field
            test_result = {
                'run_id': '555',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Pending',
                'param': 'param1\nparam2\nparam3',
                'uid': 'test_uid_555'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'RUNNING'
                }
            }

            # Execute the script
            self._run_script()

            # Verify param was split
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertEqual(ingested_data['param'], ['param1', 'param2', 'param3'])

    def test_identifier_removal_for_databricksjob(self):
        """Test identifier is removed for databricksjob sourcetype (line 38)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data with identifier field
            test_result = {
                'run_id': '666',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Pending',
                'identifier': 'should_be_removed',
                'uid': 'test_uid_666'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'RUNNING'
                }
            }

            # Execute the script
            self._run_script()

            # Verify identifier was removed
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertNotIn('identifier', ingested_data)

    def test_index_and_sourcetype_removed_from_ingestion(self):
        """Test index and sourcetype are removed from ingested data (lines 39-40)."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '777',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Pending',
                'uid': 'test_uid_777'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'RUNNING'
                }
            }

            # Execute the script
            self._run_script()

            # Verify index and sourcetype were removed from ingested data
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertNotIn('index', ingested_data)
            self.assertNotIn('sourcetype', ingested_data)
            # But they should be passed as parameters to ingest_data_to_splunk
            self.assertEqual(call_args[0][2], 'test_index')
            self.assertEqual(call_args[0][3], 'databricks:databricksjob')

    def test_multiple_results_all_processed(self):
        """Test multiple results are all processed."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data with three results
            test_results = [
                {
                    'run_id': '100',
                    'account_name': 'test_account',
                    'index': 'test_index',
                    'sourcetype': 'databricks:databricksjob',
                    'run_execution_status': 'Pending',
                    'uid': 'uid_100'
                },
                {
                    'run_id': '101',
                    'account_name': 'test_account',
                    'index': 'test_index',
                    'sourcetype': 'databricks:databricksjob',
                    'run_execution_status': 'Running',
                    'uid': 'uid_101'
                },
                {
                    'run_id': '102',
                    'account_name': 'test_account',
                    'index': 'test_index',
                    'sourcetype': 'databricks:databricksjob',
                    'run_execution_status': 'Running',
                    'uid': 'uid_102'
                }
            ]
            mock_get_results.return_value = (test_results, {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient - all state changes
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.side_effect = [
                {'state': {'life_cycle_state': 'RUNNING'}},
                {'state': {'life_cycle_state': 'TERMINATED', 'result_state': 'SUCCESS'}},
                {'state': {'life_cycle_state': 'TERMINATED', 'result_state': 'FAILED'}}
            ]

            # Execute the script
            self._run_script()

            # Verify all three results were ingested
            self.assertEqual(mock_ingest.call_count, 3)

    def test_no_uid_in_result(self):
        """Test handling when uid is not present in result."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data without uid
            test_result = {
                'run_id': '888',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Pending'
                # Note: no 'uid' field
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'RUNNING'
                }
            }

            # Execute the script - should handle missing uid gracefully
            self._run_script()

            # Verify ingestion still occurred
            mock_ingest.assert_called_once()

    def test_non_databricksjob_sourcetype(self):
        """Test that identifier is not removed for non-databricksjob sourcetype."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data with different sourcetype
            test_result = {
                'run_id': '999',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:other',  # Not databricksjob
                'run_execution_status': 'Pending',
                'identifier': 'should_be_kept',
                'uid': 'test_uid_999'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'RUNNING'
                }
            }

            # Execute the script
            self._run_script()

            # Verify identifier was NOT removed
            mock_ingest.assert_called_once()
            call_args = mock_ingest.call_args
            ingested_data = call_args[0][0]
            self.assertEqual(ingested_data['identifier'], 'should_be_kept')

    def test_pending_state_no_change(self):
        """Test PENDING state when status is already Pending."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '1000',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Pending',  # Already Pending
                'uid': 'test_uid_1000'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'PENDING'
                }
            }

            # Execute the script
            self._run_script()

            # Verify no ingestion occurred since status didn't change
            mock_ingest.assert_not_called()

    def test_other_state_no_change(self):
        """Test other state when status hasn't changed."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('time.time', return_value=1234567890.0), \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '1001',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'CUSTOM_STATE',  # Same as result_state
                'uid': 'test_uid_1001'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = {
                'state': {
                    'life_cycle_state': 'OTHER',
                    'result_state': 'CUSTOM_STATE'
                }
            }

            # Execute the script
            self._run_script()

            # Verify no ingestion occurred since status didn't change
            mock_ingest.assert_not_called()

    def test_none_response_from_api(self):
        """Test handling when API returns None response."""
        with patch('splunk.Intersplunk.getOrganizedResults') as mock_get_results, \
             patch('databricks_com.DatabricksClient') as mock_client_class, \
             patch('databricks_common_utils.ingest_data_to_splunk') as mock_ingest, \
             patch('sys.exit'):

            # Setup test data
            test_result = {
                'run_id': '1002',
                'account_name': 'test_account',
                'index': 'test_index',
                'sourcetype': 'databricks:databricksjob',
                'run_execution_status': 'Running',
                'uid': 'test_uid_1002'
            }
            mock_get_results.return_value = ([test_result], {}, {'sessionKey': 'test_session_key'})

            # Mock DatabricksClient to return None
            mock_client = MagicMock()
            mock_client_class.return_value = mock_client
            mock_client.databricks_api.return_value = None

            # Execute the script
            self._run_script()

            # Verify no ingestion occurred
            mock_ingest.assert_not_called()
