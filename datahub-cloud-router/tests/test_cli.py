"""
Unit tests for the command-line interface.
"""

from unittest.mock import Mock, patch

import pytest

from datahub.cloud.router.cli import main


class TestCLI:
    """Test cases for CLI functionality."""

    @patch("sys.argv", ["datahub-router", "--help"])
    def test_cli_help(self):
        """Test CLI help output."""
        with patch("argparse.ArgumentParser.print_help") as mock_help:
            with pytest.raises(SystemExit):
                main()
            mock_help.assert_called_once()

    @patch("sys.argv", ["datahub-router"])
    @patch("datahub.cloud.router.cli.DataHubMultiTenantRouter")
    @patch("datahub.cloud.router.cli.MultiTenantRouter")
    def test_cli_default_config(self, mock_router_class, mock_server_class):
        """Test CLI with default configuration."""
        # Mock router and server
        mock_router = Mock()
        mock_router_class.return_value = mock_router

        mock_server = Mock()
        mock_server_class.return_value = mock_server

        # Mock server.run_server to avoid actually starting the server
        mock_server.run_server = Mock()

        with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
            mock_db_config.return_value = Mock()

            main()

            # Verify router was created with default config
            mock_router_class.assert_called_once()

            # Verify server was created with router and auth parameters
            mock_server_class.assert_called_once_with(
                mock_router,
                admin_auth_enabled=True,  # Default auth enabled
                admin_api_key=None,  # No API key from env
                webhook_auth_enabled=True,  # Default webhook auth enabled
                teams_app_id=None,  # No Teams credentials from env
                teams_app_password=None,
                slack_signing_secret=None,  # No Slack credentials from env
                oauth_secret=None,  # No OAuth secret from env
            )

            # Verify server was started with default host and port
            mock_server.run_server.assert_called_once_with(host="0.0.0.0", port=9005)

    @patch("sys.argv", ["datahub-router", "--port", "8080", "--host", "127.0.0.1"])
    @patch("datahub.cloud.router.cli.DataHubMultiTenantRouter")
    @patch("datahub.cloud.router.cli.MultiTenantRouter")
    def test_cli_custom_host_port(self, mock_router_class, mock_server_class):
        """Test CLI with custom host and port."""
        # Mock router and server
        mock_router = Mock()
        mock_router_class.return_value = mock_router

        mock_server = Mock()
        mock_server_class.return_value = mock_server
        mock_server.run_server = Mock()

        with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
            mock_db_config.return_value = Mock()

            main()

            # Verify server was started with custom host and port
            mock_server.run_server.assert_called_once_with(host="127.0.0.1", port=8080)

    @patch("sys.argv", ["datahub-router", "--db-type", "inmemory"])
    @patch("datahub.cloud.router.cli.DataHubMultiTenantRouter")
    @patch("datahub.cloud.router.cli.MultiTenantRouter")
    def test_cli_inmemory_database(self, mock_router_class, mock_server_class):
        """Test CLI with in-memory database."""
        # Mock router and server
        mock_router = Mock()
        mock_router_class.return_value = mock_router

        mock_server = Mock()
        mock_server_class.return_value = mock_server
        mock_server.run_server = Mock()

        with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
            mock_db_config.return_value = Mock()

            main()

            # Verify DatabaseConfig was called with in-memory type
            mock_db_config.assert_called_once_with(type="inmemory", path=None)

    @patch(
        "sys.argv", ["datahub-router", "--db-type", "sqlite", "--db-path", "custom.db"]
    )
    @patch("datahub.cloud.router.cli.DataHubMultiTenantRouter")
    @patch("datahub.cloud.router.cli.MultiTenantRouter")
    def test_cli_sqlite_database(self, mock_router_class, mock_server_class):
        """Test CLI with SQLite database."""
        # Mock router and server
        mock_router = Mock()
        mock_router_class.return_value = mock_router

        mock_server = Mock()
        mock_server_class.return_value = mock_server
        mock_server.run_server = Mock()

        with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
            mock_db_config.return_value = Mock()

            main()

            # Verify DatabaseConfig was called with SQLite type and path
            mock_db_config.assert_called_once_with(type="sqlite", path="custom.db")

    @patch("sys.argv", ["datahub-router", "--target-url", "https://custom-datahub.com"])
    @patch("datahub.cloud.router.cli.DataHubMultiTenantRouter")
    @patch("datahub.cloud.router.cli.MultiTenantRouter")
    def test_cli_custom_target_url(self, mock_router_class, mock_server_class):
        """Test CLI with custom target URL."""
        # Mock router and server
        mock_router = Mock()
        mock_router_class.return_value = mock_router

        mock_server = Mock()
        mock_server_class.return_value = mock_server
        mock_server.run_server = Mock()

        with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
            mock_db_config.return_value = Mock()

            main()

            # Verify router's default target URL was updated
            assert mock_router.default_target_url == "https://custom-datahub.com"

    @patch("sys.argv", ["datahub-router"])
    @patch("datahub.cloud.router.cli.DataHubMultiTenantRouter")
    @patch("datahub.cloud.router.cli.MultiTenantRouter")
    def test_cli_keyboard_interrupt(self, mock_router_class, mock_server_class):
        """Test CLI handling of keyboard interrupt."""
        # Mock router and server
        mock_router = Mock()
        mock_router_class.return_value = mock_router

        mock_server = Mock()
        mock_server_class.return_value = mock_server
        mock_server.run_server = Mock(side_effect=KeyboardInterrupt())

        with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
            mock_db_config.return_value = Mock()

            with patch("builtins.print") as mock_print:
                with pytest.raises(SystemExit) as exc_info:
                    main()

                assert exc_info.value.code == 0
                mock_print.assert_called_with("\n⏹️  Server stopped by user")

    @patch("sys.argv", ["datahub-router"])
    @patch("datahub.cloud.router.cli.DataHubMultiTenantRouter")
    @patch("datahub.cloud.router.cli.MultiTenantRouter")
    def test_cli_general_exception(self, mock_router_class, mock_server_class):
        """Test CLI handling of general exceptions."""
        # Mock router and server
        mock_router = Mock()
        mock_router_class.return_value = mock_router

        mock_server = Mock()
        mock_server_class.return_value = mock_server
        mock_server.run_server = Mock(side_effect=Exception("Test error"))

        with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
            mock_db_config.return_value = Mock()

            with patch("builtins.print") as mock_print:
                with pytest.raises(SystemExit) as exc_info:
                    main()

                assert exc_info.value.code == 1
                mock_print.assert_called_with("❌ Error starting server: Test error")

    @patch.dict("os.environ", {"DATAHUB_ROUTER_DB_TYPE": "mysql"})
    @patch("sys.argv", ["datahub-router"])
    @patch("datahub.cloud.router.cli.DataHubMultiTenantRouter")
    @patch("datahub.cloud.router.cli.MultiTenantRouter")
    def test_cli_environment_variables(self, mock_router_class, mock_server_class):
        """Test CLI with environment variables."""
        # Mock router and server
        mock_router = Mock()
        mock_router_class.return_value = mock_router

        mock_server = Mock()
        mock_server_class.return_value = mock_server
        mock_server.run_server = Mock()

        with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
            mock_db_config.return_value = Mock()

            main()

            # Verify DatabaseConfig was called with environment variable value
            mock_db_config.assert_called_once_with(type="mysql", path=None)

    @patch.dict("os.environ", {"DATAHUB_ROUTER_TARGET_URL": "https://env-datahub.com"})
    @patch("sys.argv", ["datahub-router"])
    @patch("datahub.cloud.router.cli.DataHubMultiTenantRouter")
    @patch("datahub.cloud.router.cli.MultiTenantRouter")
    def test_cli_environment_target_url(self, mock_router_class, mock_server_class):
        """Test CLI with target URL from environment variable."""
        # Mock router and server
        mock_router = Mock()
        mock_router_class.return_value = mock_router

        mock_server = Mock()
        mock_server_class.return_value = mock_server
        mock_server.run_server = Mock()

        with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
            mock_db_config.return_value = Mock()

            main()

            # Verify router's default target URL was updated from environment
            assert mock_router.default_target_url == "https://env-datahub.com"

    @patch("sys.argv", ["datahub-router", "--db-type", "mysql"])
    def test_cli_mysql_database_type(self):
        """Test CLI with MySQL database type."""
        with patch(
            "datahub.cloud.router.cli.DataHubMultiTenantRouter"
        ) as mock_server_class:
            with patch(
                "datahub.cloud.router.cli.MultiTenantRouter"
            ) as mock_router_class:
                with patch("datahub.cloud.router.cli.DatabaseConfig") as mock_db_config:
                    # Mock router and server
                    mock_router = Mock()
                    mock_router_class.return_value = mock_router

                    mock_server = Mock()
                    mock_server_class.return_value = mock_server
                    mock_server.run_server = Mock()

                    main()

                    # Verify DatabaseConfig was called with MySQL type
                    mock_db_config.assert_called_once_with(type="mysql", path=None)

    def test_cli_argument_parser(self):
        """Test CLI argument parser configuration."""
        with patch("sys.argv", ["datahub-router", "--help"]):
            with patch("argparse.ArgumentParser.parse_args") as mock_parse:
                mock_args = Mock()
                mock_args.host = "0.0.0.0"
                mock_args.port = 9005
                mock_args.db_type = "sqlite"
                mock_args.db_path = ".dev/router.db"
                mock_args.target_url = "http://localhost:9003"
                mock_args.reload = False
                mock_parse.return_value = mock_args

                with patch(
                    "datahub.cloud.router.cli.DataHubMultiTenantRouter"
                ) as mock_server_class:
                    with patch(
                        "datahub.cloud.router.cli.MultiTenantRouter"
                    ) as mock_router_class:
                        with patch("datahub.cloud.router.cli.DatabaseConfig"):
                            # Mock router and server
                            mock_router = Mock()
                            mock_router_class.return_value = mock_router

                            mock_server = Mock()
                            mock_server_class.return_value = mock_server
                            mock_server.run_server = Mock()

                            main()

                            # Verify arguments were parsed correctly
                            mock_parse.assert_called_once()
