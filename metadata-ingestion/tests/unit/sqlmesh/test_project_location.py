import pathlib
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from datahub.configuration.git import GitInfo
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.sqlmesh.project_location import (
    _safe_join,
    resolve_project_location,
)
from datahub.ingestion.source.sqlmesh.sqlmesh_config import (
    SqlmeshSourceConfig,
    SqlmeshSourceReport,
)


def _config(**overrides: object) -> SqlmeshSourceConfig:
    return SqlmeshSourceConfig.model_validate(
        {"target_platform": "snowflake", **overrides}
    )


class TestResolveProjectLocation:
    def test_local_path_passthrough(self) -> None:
        config = _config(project_path="/opt/sqlmesh_project")
        report = SqlmeshSourceReport()
        with ExitStack() as stack:
            resolved = resolve_project_location(config, report, stack)
        assert resolved == "/opt/sqlmesh_project"
        assert report.git_checkout is None
        assert report.num_project_files_downloaded == 0

    def test_git_info_clones_and_joins_subdir(self, tmp_path: pathlib.Path) -> None:
        checkout = tmp_path / "checkout"
        (checkout / "sqlmesh").mkdir(parents=True)
        config = _config(
            git_info={"repo": "https://github.com/org/repo", "deploy_key": "k"},
            project_path="sqlmesh",
        )
        report = SqlmeshSourceReport()
        with patch.object(GitInfo, "clone", return_value=checkout) as clone:
            with ExitStack() as stack:
                resolved = resolve_project_location(config, report, stack)
        clone.assert_called_once()
        assert resolved == str((checkout / "sqlmesh").resolve())
        assert report.git_checkout == str(checkout)

    def test_s3_prefix_downloads_tree_preserving_layout(
        self, tmp_path: pathlib.Path
    ) -> None:
        config = _config(
            project_path="s3://my-bucket/proj",
            aws_connection={"aws_region": "us-east-1"},
        )
        report = SqlmeshSourceReport()

        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "proj/config.yaml"},
                    {"Key": "proj/models/orders.sql"},
                    {"Key": "proj/"},  # directory placeholder — skipped
                ]
            }
        ]
        client.get_paginator.return_value = paginator

        def _fake_download(bucket: str, key: str, dest: str) -> None:
            pathlib.Path(dest).write_text(key)

        client.download_file.side_effect = _fake_download

        with patch.object(AwsConnectionConfig, "get_s3_client", return_value=client):
            with ExitStack() as stack:
                resolved = resolve_project_location(config, report, stack)
                root = pathlib.Path(resolved)
                assert (root / "config.yaml").is_file()
                assert (root / "models" / "orders.sql").is_file()

        assert report.num_project_files_downloaded == 2
        paginator.paginate.assert_called_once_with(Bucket="my-bucket", Prefix="proj/")

    def test_s3_prefix_with_no_objects_raises(self) -> None:
        config = _config(
            project_path="s3://my-bucket/empty",
            aws_connection={"aws_region": "us-east-1"},
        )
        report = SqlmeshSourceReport()
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{}]  # no Contents
        client.get_paginator.return_value = paginator

        with patch.object(AwsConnectionConfig, "get_s3_client", return_value=client):
            with ExitStack() as stack:
                with pytest.raises(ValueError, match="No objects found"):
                    resolve_project_location(config, report, stack)


class TestSafeJoin:
    def test_rejects_parent_traversal(self, tmp_path: pathlib.Path) -> None:
        with pytest.raises(ValueError, match="escapes"):
            _safe_join(tmp_path, "../../etc/passwd")

    def test_allows_nested_child(self, tmp_path: pathlib.Path) -> None:
        assert (
            _safe_join(tmp_path, "models/a.sql")
            == (tmp_path / "models" / "a.sql").resolve()
        )


class TestConfigValidation:
    def test_s3_project_path_requires_aws_connection(self) -> None:
        with pytest.raises(ValidationError, match="aws_connection is required"):
            _config(project_path="s3://bucket/proj")

    def test_git_info_rejects_s3_project_path(self) -> None:
        with pytest.raises(ValidationError, match="cannot be an s3:// URI"):
            _config(
                git_info={"repo": "https://github.com/org/repo", "deploy_key": "k"},
                project_path="s3://bucket/proj",
                aws_connection={"aws_region": "us-east-1"},
            )

    @pytest.mark.parametrize("bare", ["s3://bucket", "s3://bucket/"])
    def test_s3_bare_bucket_rejected(self, bare: str) -> None:
        # A bare bucket would download the whole bucket into a temp dir.
        with pytest.raises(ValidationError, match="key prefix"):
            _config(
                project_path=bare,
                aws_connection={"aws_region": "us-east-1"},
            )
