import json
import os
import tempfile
import zipfile
from unittest.mock import MagicMock, patch

from datahub_integrations.teams.app_manifest import (
    _create_fallback_icons,
    generate_teams_app_package,
    get_teams_app_manifest,
)


class TestTeamsAppManifest:
    """Test suite for Teams app manifest generation."""

    def test_get_teams_app_manifest_basic(self) -> None:
        """Test basic Teams app manifest generation."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        manifest = get_teams_app_manifest(app_id, webhook_url)

        assert manifest["id"] == app_id
        assert manifest["manifestVersion"] == "1.16"
        assert manifest["version"] == "1.3.5"
        assert manifest["name"]["short"] == "DataHub"
        assert manifest["name"]["full"] == "DataHub - Modern Data Discovery"
        assert manifest["developer"]["name"] == "DataHub"
        assert manifest["developer"]["websiteUrl"] == "https://datahub.com"
        assert manifest["accentColor"] == "#142f39"

        # Check bot configuration
        assert len(manifest["bots"]) == 1
        bot = manifest["bots"][0]
        assert bot["botId"] == app_id
        assert bot["scopes"] == ["personal", "team", "groupchat"]
        assert not bot["supportsFiles"]
        assert not bot["isNotificationOnly"]

        # Check command lists
        assert len(bot["commandLists"]) == 1
        command_list = bot["commandLists"][0]
        assert command_list["scopes"] == ["personal", "team", "groupchat"]
        assert len(command_list["commands"]) == 4

        # Check compose extensions
        assert len(manifest["composeExtensions"]) == 1
        compose_ext = manifest["composeExtensions"][0]
        assert compose_ext["botId"] == app_id
        assert len(compose_ext["commands"]) == 1

        # Check activities
        assert "activities" in manifest
        assert len(manifest["activities"]["activityTypes"]) == 5

        # Check permissions
        assert "identity" in manifest["permissions"]
        assert "messageTeamMembers" in manifest["permissions"]

        # Check valid domains
        assert "*.datahub.com" in manifest["validDomains"]
        assert "*.acryl.io" in manifest["validDomains"]

    def test_get_teams_app_manifest_with_icon_url(self) -> None:
        """Test Teams app manifest generation with icon URL."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"
        icon_url = "https://example.com/icon.png"

        manifest = get_teams_app_manifest(app_id, webhook_url, icon_url)

        assert manifest["id"] == app_id
        assert manifest["icons"]["color"] == "color.png"
        assert manifest["icons"]["outline"] == "outline.png"

    def test_get_teams_app_manifest_command_structure(self) -> None:
        """Test command structure in Teams app manifest."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        manifest = get_teams_app_manifest(app_id, webhook_url)

        commands = manifest["bots"][0]["commandLists"][0]["commands"]
        command_titles = [cmd["title"] for cmd in commands]

        assert "Ask Question" in command_titles
        assert "Get Entity" in command_titles
        assert "Search" in command_titles
        assert "Help" in command_titles

        # Check compose extension commands
        compose_commands = manifest["composeExtensions"][0]["commands"]
        assert len(compose_commands) == 1
        assert compose_commands[0]["id"] == "searchDataHub"
        assert compose_commands[0]["title"] == "Search DataHub"
        assert compose_commands[0]["type"] == "query"

    def test_get_teams_app_manifest_activity_types(self) -> None:
        """Test activity types in Teams app manifest."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        manifest = get_teams_app_manifest(app_id, webhook_url)

        activity_types = manifest["activities"]["activityTypes"]
        type_names = [activity["type"] for activity in activity_types]

        assert "datasetUpdated" in type_names
        assert "entityChanged" in type_names
        assert "incidentNotification" in type_names
        assert "complianceNotification" in type_names
        assert "generalNotification" in type_names

        # Check template text
        dataset_activity = next(
            a for a in activity_types if a["type"] == "datasetUpdated"
        )
        assert "{datasetName}" in dataset_activity["templateText"]

    def test_get_teams_app_manifest_web_app_info(self) -> None:
        """Test webApplicationInfo in Teams app manifest."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        manifest = get_teams_app_manifest(app_id, webhook_url)

        web_app_info = manifest["webApplicationInfo"]
        assert web_app_info["id"] == app_id
        assert web_app_info["resource"] == "https://graph.microsoft.com/"

    @patch("os.path.exists")
    @patch("builtins.open")
    def test_generate_teams_app_package_basic(
        self, mock_open: MagicMock, mock_exists: MagicMock
    ) -> None:
        """Test basic Teams app package generation."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        # Mock no existing DataHub logo
        mock_exists.return_value = False

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch(
                "datahub_integrations.teams.app_manifest._create_fallback_icons"
            ) as mock_create_icons:
                with patch("zipfile.ZipFile") as mock_zipfile:
                    mock_zip = MagicMock()
                    mock_zipfile.return_value.__enter__.return_value = mock_zip

                    result_path = generate_teams_app_package(
                        app_id, webhook_url, output_dir=temp_dir
                    )

                    # Check result path
                    expected_path = os.path.join(
                        temp_dir, f"datahub-teams-app-{app_id}-v1.3.5.zip"
                    )
                    assert result_path == expected_path

                    # Check ZIP file creation
                    mock_zipfile.assert_called_once_with(
                        expected_path, "w", zipfile.ZIP_DEFLATED
                    )

                    # Check files added to ZIP
                    assert mock_zip.write.call_count == 3
                    call_args = [call.args[1] for call in mock_zip.write.call_args_list]
                    assert "manifest.json" in call_args
                    assert "color.png" in call_args
                    assert "outline.png" in call_args

                    # Check fallback icons were created
                    mock_create_icons.assert_called_once()

    @patch("os.path.exists")
    @patch("builtins.open")
    def test_generate_teams_app_package_with_existing_logo(
        self, mock_open: MagicMock, mock_exists: MagicMock
    ) -> None:
        """Test Teams app package generation with existing DataHub logo."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        # Mock existing DataHub logo
        mock_exists.side_effect = (
            lambda path: path == "datahub-web-react/src/images/datahublogo.png"
        )

        # Mock file reading
        mock_logo_data = b"fake_logo_data"
        mock_file = MagicMock()
        mock_file.read.return_value = mock_logo_data
        mock_open.return_value.__enter__.return_value = mock_file

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("datahub_integrations.teams.app_manifest.PIL_AVAILABLE", True):
                with patch(
                    "datahub_integrations.teams.app_manifest.Image"
                ) as mock_image:
                    mock_img = MagicMock()
                    mock_image.open.return_value = mock_img
                    mock_img.resize.return_value = mock_img
                    mock_img.convert.return_value = mock_img
                    mock_img.getdata.return_value = [(255, 255, 255, 255)] * 10

                    with patch("zipfile.ZipFile") as mock_zipfile:
                        mock_zip = MagicMock()
                        mock_zipfile.return_value.__enter__.return_value = mock_zip

                        result_path = generate_teams_app_package(
                            app_id, webhook_url, output_dir=temp_dir
                        )

                        # Check result path
                        expected_path = os.path.join(
                            temp_dir, f"datahub-teams-app-{app_id}-v1.3.5.zip"
                        )
                        assert result_path == expected_path

                        # Check ZIP file creation
                        mock_zipfile.assert_called_once()

                        # Check image processing
                        mock_image.open.assert_called_once()
                        assert (
                            mock_img.resize.call_count == 2
                        )  # color and outline icons

    @patch("os.path.exists")
    @patch("builtins.open")
    def test_generate_teams_app_package_no_pil(
        self, mock_open: MagicMock, mock_exists: MagicMock
    ) -> None:
        """Test Teams app package generation without PIL."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        # Mock existing DataHub logo
        mock_exists.side_effect = (
            lambda path: path == "datahub-web-react/src/images/datahublogo.png"
        )

        # Mock file reading
        mock_logo_data = b"fake_logo_data"
        mock_file = MagicMock()
        mock_file.read.return_value = mock_logo_data
        mock_open.return_value.__enter__.return_value = mock_file

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("datahub_integrations.teams.app_manifest.PIL_AVAILABLE", False):
                with patch("zipfile.ZipFile") as mock_zipfile:
                    mock_zip = MagicMock()
                    mock_zipfile.return_value.__enter__.return_value = mock_zip

                    result_path = generate_teams_app_package(
                        app_id, webhook_url, output_dir=temp_dir
                    )

                    # Check result path
                    expected_path = os.path.join(
                        temp_dir, f"datahub-teams-app-{app_id}-v1.3.5.zip"
                    )
                    assert result_path == expected_path

                    # Check ZIP file creation
                    mock_zipfile.assert_called_once()

    @patch("datahub_integrations.teams.app_manifest.PIL_AVAILABLE", True)
    @patch("datahub_integrations.teams.app_manifest.Image")
    def test_create_fallback_icons_with_pil(self, mock_image: MagicMock) -> None:
        """Test fallback icon creation with PIL available."""
        color_icon_path = "/tmp/color.png"
        outline_icon_path = "/tmp/outline.png"

        # Mock PIL objects
        mock_color_img = MagicMock()
        mock_outline_img = MagicMock()
        mock_draw = MagicMock()
        mock_font = MagicMock()

        with patch(
            "datahub_integrations.teams.app_manifest.ImageDraw"
        ) as mock_image_draw:
            with patch(
                "datahub_integrations.teams.app_manifest.ImageFont"
            ) as mock_image_font:
                mock_image_draw.Draw.return_value = mock_draw
                mock_image_font.truetype.return_value = mock_font
                mock_image_font.load_default.return_value = mock_font
                mock_image.new.side_effect = [mock_color_img, mock_outline_img]

                # Mock text bounding box
                mock_draw.textbbox.return_value = (0, 0, 30, 20)

                _create_fallback_icons(color_icon_path, outline_icon_path)

                # Check image creation
                assert mock_image.new.call_count == 2

                # Check drawing operations
                assert mock_draw.ellipse.call_count == 2
                assert mock_draw.text.call_count == 2

                # Check save operations
                mock_color_img.save.assert_called_once_with(color_icon_path, "PNG")
                mock_outline_img.save.assert_called_once_with(
                    outline_icon_path, "PNG", transparency=0
                )

    @patch("datahub_integrations.teams.app_manifest.PIL_AVAILABLE", False)
    @patch("builtins.open")
    def test_create_fallback_icons_without_pil(self, mock_open: MagicMock) -> None:
        """Test fallback icon creation without PIL."""
        color_icon_path = "/tmp/color.png"
        outline_icon_path = "/tmp/outline.png"

        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        _create_fallback_icons(color_icon_path, outline_icon_path)

        # Check files were written
        assert mock_open.call_count == 2
        assert mock_file.write.call_count == 2

    def test_generate_teams_app_package_custom_output_dir(self) -> None:
        """Test Teams app package generation with custom output directory."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        with tempfile.TemporaryDirectory() as temp_dir:
            custom_output_dir = os.path.join(temp_dir, "custom")
            os.makedirs(custom_output_dir)

            with patch(
                "datahub_integrations.teams.app_manifest._create_fallback_icons"
            ):
                with patch("zipfile.ZipFile") as mock_zipfile:
                    mock_zip = MagicMock()
                    mock_zipfile.return_value.__enter__.return_value = mock_zip

                    result_path = generate_teams_app_package(
                        app_id, webhook_url, output_dir=custom_output_dir
                    )

                    # Check result path uses custom directory
                    expected_path = os.path.join(
                        custom_output_dir, f"datahub-teams-app-{app_id}-v1.3.5.zip"
                    )
                    assert result_path == expected_path

    def test_generate_teams_app_package_manifest_content(self) -> None:
        """Test manifest content in generated package."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch(
                "datahub_integrations.teams.app_manifest._create_fallback_icons"
            ):
                with patch("zipfile.ZipFile") as mock_zipfile:
                    mock_zip = MagicMock()
                    mock_zipfile.return_value.__enter__.return_value = mock_zip

                    # Capture manifest content
                    written_files = {}

                    def capture_write(file_path: str, zip_path: str) -> None:
                        if zip_path == "manifest.json":
                            # Read the written manifest file
                            with open(file_path, "r") as f:
                                written_files[zip_path] = json.load(f)

                    mock_zip.write.side_effect = capture_write

                    generate_teams_app_package(app_id, webhook_url, output_dir=temp_dir)

                    # Check manifest was written with correct content
                    assert "manifest.json" in written_files
                    manifest = written_files["manifest.json"]
                    assert manifest["id"] == app_id
                    assert manifest["version"] == "1.3.5"

    def test_generate_teams_app_package_version_in_filename(self) -> None:
        """Test that package filename includes version from manifest."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch(
                "datahub_integrations.teams.app_manifest._create_fallback_icons"
            ):
                with patch("zipfile.ZipFile") as mock_zipfile:
                    mock_zip = MagicMock()
                    mock_zipfile.return_value.__enter__.return_value = mock_zip

                    result_path = generate_teams_app_package(
                        app_id, webhook_url, output_dir=temp_dir
                    )

                    # Check filename contains version
                    assert "v1.3.5" in result_path
                    assert result_path.endswith(".zip")

    def test_manifest_schema_compliance(self) -> None:
        """Test that manifest complies with Teams schema requirements."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        manifest = get_teams_app_manifest(app_id, webhook_url)

        # Check required fields
        required_fields = [
            "$schema",
            "manifestVersion",
            "version",
            "id",
            "developer",
            "icons",
            "name",
            "description",
            "accentColor",
            "bots",
        ]

        for field in required_fields:
            assert field in manifest, f"Required field '{field}' missing from manifest"

        # Check developer required fields
        developer_required = ["name", "websiteUrl", "privacyUrl", "termsOfUseUrl"]
        for field in developer_required:
            assert field in manifest["developer"], (
                f"Required developer field '{field}' missing"
            )

        # Check schema version
        assert (
            manifest["$schema"]
            == "https://developer.microsoft.com/en-us/json-schemas/teams/v1.16/MicrosoftTeams.schema.json"
        )
        assert manifest["manifestVersion"] == "1.16"

    def test_manifest_bot_configuration(self) -> None:
        """Test bot configuration in manifest."""
        app_id = "test-app-id"
        webhook_url = "https://example.com/webhook"

        manifest = get_teams_app_manifest(app_id, webhook_url)

        bot = manifest["bots"][0]

        # Check bot properties
        assert bot["botId"] == app_id
        assert "personal" in bot["scopes"]
        assert "team" in bot["scopes"]
        assert "groupchat" in bot["scopes"]
        assert bot["supportsFiles"] is False
        assert bot["isNotificationOnly"] is False

        # Check command structure
        assert len(bot["commandLists"]) == 1
        command_list = bot["commandLists"][0]
        assert len(command_list["commands"]) == 4

        # Check each command has required fields
        for command in command_list["commands"]:
            assert "title" in command
            assert "description" in command
