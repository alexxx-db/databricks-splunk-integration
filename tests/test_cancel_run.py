import declare
import unittest
import json
from mock import patch, MagicMock

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
        'solnlib.server_info',
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}

    # Create a proper mock for PersistentServerConnectionApplication
    class MockPersistentServerConnectionApplication:
        def __init__(self):
            pass

    # Mock the splunk.persistconn.application module
    mock_persistconn_app = MagicMock()
    mock_persistconn_app.PersistentServerConnectionApplication = MockPersistentServerConnectionApplication

    mock_persistconn = MagicMock()
    mock_persistconn.application = mock_persistconn_app

    mocked_modules['splunk.persistconn'] = mock_persistconn
    mocked_modules['splunk.persistconn.application'] = mock_persistconn_app

    for module, magicmock in mocked_modules.items():
        patch.dict('sys.modules', **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()


class TestCancelRunningExecution(unittest.TestCase):
    """Test CancelRunningExecution class."""

    def setUp(self):
        """Set up the test."""
        import cancel_run
        self.cancel_run = cancel_run
        self.CancelRunningExecution = cancel_run.CancelRunningExecution

    def test_initialization(self):
        """Test object initialization with all attributes."""
        obj = self.CancelRunningExecution("command_line", "command_arg")

        # Verify all attributes are initialized to None or empty dict
        self.assertIsNone(obj.run_id)
        self.assertIsNone(obj.account_name)
        self.assertIsNone(obj.uid)
        self.assertEqual(obj.payload, {})
        self.assertIsNone(obj.status)
        self.assertIsNone(obj.session_key)

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_successful_cancellation(self, mock_client_class):
        """Test successful run cancellation with 200 response."""
        # Setup
        obj = self.CancelRunningExecution("command_line", "command_arg")
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.databricks_api.return_value = ({"status": "CANCELLED"}, 200)

        # Prepare input data
        input_data = json.dumps({
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
                "uid": "test_uid_123"
            },
            "session": {
                "authtoken": "test_session_key"
            }
        })

        # Execute
        result = obj.handle(input_data)

        # Verify
        self.assertEqual(result['status'], 200)
        self.assertEqual(result['payload']['canceled'], "Success")
        self.assertEqual(obj.run_id, "12345")
        self.assertEqual(obj.account_name, "test_account")
        self.assertEqual(obj.uid, "test_uid_123")
        self.assertEqual(obj.session_key, "test_session_key")

        # Verify DatabricksClient was called correctly
        mock_client_class.assert_called_once_with("test_account", "test_session_key")
        mock_client.databricks_api.assert_called_once_with(
            "post",
            "/api/2.0/jobs/runs/cancel",
            data={"run_id": "12345"}
        )

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_failed_cancellation_404(self, mock_client_class):
        """Test failed cancellation with 404 response."""
        # Setup
        obj = self.CancelRunningExecution("command_line", "command_arg")
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.databricks_api.return_value = ({"error": "Not found"}, 404)

        # Prepare input data
        input_data = json.dumps({
            "form": {
                "run_id": "99999",
                "account_name": "test_account",
                "uid": "test_uid_123"
            },
            "session": {
                "authtoken": "test_session_key"
            }
        })

        # Execute
        result = obj.handle(input_data)

        # Verify
        self.assertEqual(result['status'], 500)
        self.assertEqual(result['payload']['canceled'], "Failed")
        mock_client.databricks_api.assert_called_once()

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_failed_cancellation_500(self, mock_client_class):
        """Test failed cancellation with 500 response."""
        # Setup
        obj = self.CancelRunningExecution("command_line", "command_arg")
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.databricks_api.return_value = ({"error": "Internal server error"}, 500)

        # Prepare input data
        input_data = json.dumps({
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
                "uid": "test_uid_123"
            },
            "session": {
                "authtoken": "test_session_key"
            }
        })

        # Execute
        result = obj.handle(input_data)

        # Verify
        self.assertEqual(result['status'], 500)
        self.assertEqual(result['payload']['canceled'], "Failed")

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_exception_during_api_call(self, mock_client_class):
        """Test exception raised during API call."""
        # Setup
        obj = self.CancelRunningExecution("command_line", "command_arg")
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.databricks_api.side_effect = Exception("Connection timeout")

        # Prepare input data
        input_data = json.dumps({
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
                "uid": "test_uid_123"
            },
            "session": {
                "authtoken": "test_session_key"
            }
        })

        # Execute
        result = obj.handle(input_data)

        # Verify
        self.assertEqual(result['status'], 500)
        self.assertEqual(result['payload']['canceled'], "Failed")
        mock_client.databricks_api.assert_called_once()

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_invalid_json_input(self, mock_client_class):
        """Test exception during input parsing with invalid JSON.

        Note: This test exposes a bug in the code where LOG_PREFIX is referenced
        before assignment in the outer exception handler.
        """
        # Setup
        obj = self.CancelRunningExecution("command_line", "command_arg")

        # Prepare invalid input data
        input_data = "invalid json string {{"

        # Execute - this will raise UnboundLocalError due to bug in cancel_run.py
        with self.assertRaises(UnboundLocalError):
            result = obj.handle(input_data)

        # DatabricksClient should not be instantiated
        mock_client_class.assert_not_called()

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_missing_form_data(self, mock_client_class):
        """Test exception when form data is missing.

        Note: This test exposes a bug in the code where LOG_PREFIX is referenced
        before assignment in the outer exception handler.
        """
        # Setup
        obj = self.CancelRunningExecution("command_line", "command_arg")

        # Prepare input data without form key
        input_data = json.dumps({
            "session": {
                "authtoken": "test_session_key"
            }
        })

        # Execute - this will raise UnboundLocalError due to bug in cancel_run.py
        with self.assertRaises(UnboundLocalError):
            result = obj.handle(input_data)

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_missing_session_data(self, mock_client_class):
        """Test exception when session data is missing."""
        # Setup
        obj = self.CancelRunningExecution("command_line", "command_arg")

        # Prepare input data without session key
        input_data = json.dumps({
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
                "uid": "test_uid_123"
            }
        })

        # Execute
        result = obj.handle(input_data)

        # Verify
        self.assertEqual(result['status'], 500)
        self.assertEqual(result['payload']['canceled'], "Failed")

    def test_handleStream_raises_not_implemented_error(self):
        """Test that handleStream method raises NotImplementedError."""
        obj = self.CancelRunningExecution("command_line", "command_arg")

        with self.assertRaises(NotImplementedError) as context:
            obj.handleStream("handle", "in_string")

        self.assertEqual(
            str(context.exception),
            "PersistentServerConnectionApplication.handleStream"
        )

    def test_done_method_executes_without_error(self):
        """Test that done method executes without error."""
        obj = self.CancelRunningExecution("command_line", "command_arg")

        # Should not raise any exception
        try:
            result = obj.done()
            # done() method should return None (implicitly by pass statement)
            self.assertIsNone(result)
        except Exception as e:
            self.fail(f"done() method raised an exception: {e}")

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_all_form_fields_populated(self, mock_client_class):
        """Test that all form fields are correctly extracted and stored."""
        # Setup
        obj = self.CancelRunningExecution("command_line", "command_arg")
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.databricks_api.return_value = ({"status": "CANCELLED"}, 200)

        # Prepare input data with all fields
        input_data = json.dumps({
            "form": {
                "run_id": "67890",
                "account_name": "prod_account",
                "uid": "unique_user_id_456"
            },
            "session": {
                "authtoken": "super_secret_token"
            }
        })

        # Execute
        result = obj.handle(input_data)

        # Verify all instance variables are correctly set
        self.assertEqual(obj.run_id, "67890")
        self.assertEqual(obj.account_name, "prod_account")
        self.assertEqual(obj.uid, "unique_user_id_456")
        self.assertEqual(obj.session_key, "super_secret_token")
        self.assertEqual(result['status'], 200)

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_client_instantiation_error(self, mock_client_class):
        """Test exception during DatabricksClient instantiation."""
        # Setup
        obj = self.CancelRunningExecution("command_line", "command_arg")
        mock_client_class.side_effect = Exception("Failed to create client")

        # Prepare input data
        input_data = json.dumps({
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
                "uid": "test_uid_123"
            },
            "session": {
                "authtoken": "test_session_key"
            }
        })

        # Execute
        result = obj.handle(input_data)

        # Verify
        self.assertEqual(result['status'], 500)
        self.assertEqual(result['payload']['canceled'], "Failed")
        mock_client_class.assert_called_once()

    @patch("cancel_run.com.DatabricksClient")
    def test_handle_response_status_codes(self, mock_client_class):
        """Test various non-200 status codes from API."""
        test_cases = [
            (201, 500),  # API returns 201, should result in 500 status
            (400, 500),  # Bad request
            (401, 500),  # Unauthorized
            (403, 500),  # Forbidden
            (503, 500),  # Service unavailable
        ]

        for api_status, expected_status in test_cases:
            with self.subTest(api_status=api_status):
                obj = self.CancelRunningExecution("command_line", "command_arg")
                mock_client = MagicMock()
                mock_client_class.return_value = mock_client
                mock_client.databricks_api.return_value = ({"error": "error"}, api_status)

                input_data = json.dumps({
                    "form": {
                        "run_id": "12345",
                        "account_name": "test_account",
                        "uid": "test_uid_123"
                    },
                    "session": {
                        "authtoken": "test_session_key"
                    }
                })

                result = obj.handle(input_data)

                self.assertEqual(result['status'], expected_status)
                self.assertEqual(result['payload']['canceled'], "Failed")
