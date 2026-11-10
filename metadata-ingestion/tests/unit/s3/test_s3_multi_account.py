from typing import Any, Dict, Iterable, Iterator, List, Set
from unittest.mock import patch

import pytest
from boto3.session import Session
from pydantic import ValidationError

from datahub.emitter.mce_builder import make_dataplatform_instance_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws.aws_common import (
    AwsAssumeRoleConfig,
    AwsEnvironment,
)
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.s3_probe import s3_probe_refusal
from datahub.ingestion.source.s3.source import S3Source
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
)

ACCOUNT_A = "111111111111"
ACCOUNT_B = "222222222222"
ROLE_A = f"arn:aws:iam::{ACCOUNT_A}:role/DataHubS3Reader"
ROLE_B = f"arn:aws:iam::{ACCOUNT_B}:role/DataHubS3Reader"
# Roles are only assumed from the default credential chain, so the base identity
# comes from the environment rather than aws_config.
AWS_CONFIG = {"aws_region": "us-east-1"}
FOLDERS_ONLY_SPECS = [
    {"include": "s3://*/", "emit_folders_only": True},
    {"include": "s3://*/*/", "emit_folders_only": True},
]


@pytest.fixture(autouse=True)
def default_chain_credentials(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    # Keep role assumption off the instance-metadata endpoint, which moto rejects.
    with (
        patch(
            "datahub.ingestion.source.aws.aws_common.get_current_identity",
            return_value=(None, None),
        ),
        patch(
            "datahub.ingestion.source.aws.aws_common.detect_aws_environment",
            return_value=AwsEnvironment.UNKNOWN,
        ),
    ):
        yield


def _config(**overrides: Any) -> Dict[str, Any]:
    return {
        "path_specs": FOLDERS_ONLY_SPECS,
        "aws_config": AWS_CONFIG,
        "aws_accounts": [{"aws_role": ROLE_A}, {"aws_role": ROLE_B}],
        **overrides,
    }


def _seed_account(s3: Session, role_arn: str, bucket: str, keys: List[str]) -> None:
    # moto keeps buckets per account and maps assumed-role credentials to the
    # role's account, so ListBuckets only sees what was created here.
    creds = s3.client("sts").assume_role(RoleArn=role_arn, RoleSessionName="seed")[
        "Credentials"
    ]
    client = Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name="us-east-1",
    ).client("s3")
    client.create_bucket(Bucket=bucket)
    for key in keys:
        client.put_object(Bucket=bucket, Key=key, Body=b"x")


def _containers_by_name(wus: Iterable[MetadataWorkUnit]) -> Dict[str, Dict[str, Any]]:
    aspects_by_urn: Dict[str, Dict[str, Any]] = {}
    for wu in wus:
        assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
        assert wu.metadata.entityUrn and wu.metadata.aspect
        aspects_by_urn.setdefault(wu.metadata.entityUrn, {})[
            type(wu.metadata.aspect).__name__
        ] = wu.metadata.aspect
    return {
        aspects["ContainerPropertiesClass"].name: aspects
        for aspects in aspects_by_urn.values()
        if isinstance(aspects.get("ContainerPropertiesClass"), ContainerPropertiesClass)
    }


def _instance(aspects: Dict[str, Any]) -> str:
    platform_instance = aspects["DataPlatformInstanceClass"]
    assert isinstance(platform_instance, DataPlatformInstanceClass)
    assert platform_instance.instance
    return platform_instance.instance


def test_each_account_is_listed_with_its_own_role_and_instance(s3: Session) -> None:
    _seed_account(s3, ROLE_A, "bucket-a", ["raw/file.csv"])
    _seed_account(s3, ROLE_B, "bucket-b", ["curated/file.csv"])
    _seed_account(s3, ROLE_B, "empty-bucket", [])

    source = S3Source.create(_config(), PipelineContext(run_id="multi-account"))
    containers = _containers_by_name(source.get_workunits_internal())

    assert set(containers) == {"bucket-a", "raw", "bucket-b", "curated", "empty-bucket"}
    instance_a = make_dataplatform_instance_urn("s3", ACCOUNT_A)
    instance_b = make_dataplatform_instance_urn("s3", ACCOUNT_B)
    assert _instance(containers["bucket-a"]) == instance_a
    assert _instance(containers["raw"]) == instance_a
    assert _instance(containers["bucket-b"]) == instance_b
    assert _instance(containers["empty-bucket"]) == instance_b

    # Buckets are root containers, so the account level of the browse tree comes
    # from their own browse path; folders inherit it through their container.
    for bucket, instance in [("bucket-a", instance_a), ("bucket-b", instance_b)]:
        browse_path = containers[bucket]["BrowsePathsV2Class"]
        assert isinstance(browse_path, BrowsePathsV2Class)
        assert [entry.urn for entry in browse_path.path] == [instance]
    assert "BrowsePathsV2Class" not in containers["raw"]

    assert source.report.aws_accounts_ingested == 2
    assert not source.report.failures
    # The recipe-level config is restored after each account.
    assert source.source_config.platform_instance is None


def test_failed_account_is_reported_and_others_still_ingest(s3: Session) -> None:
    _seed_account(s3, ROLE_A, "bucket-a", ["raw/file.csv"])
    _seed_account(s3, ROLE_B, "bucket-b", ["curated/file.csv"])

    source = S3Source.create(_config(), PipelineContext(run_id="multi-account"))
    process_folders = source._process_folders

    def deny_account_a(path_spec: Any) -> Iterable[MetadataWorkUnit]:
        if source.source_config.platform_instance == ACCOUNT_A:
            raise PermissionError("AccessDenied")
        return process_folders(path_spec)

    with patch.object(source, "_process_folders", side_effect=deny_account_a):
        containers = _containers_by_name(source.get_workunits_internal())

    assert set(containers) == {"bucket-b", "curated"}
    assert source.report.aws_accounts_ingested == 1
    assert list(source.report.aws_accounts_failed) == [ACCOUNT_A]
    # A reported failure is what keeps stale-entity removal from soft-deleting
    # the failed account's metadata.
    assert len(source.report.failures) == 1


def test_account_role_is_chained_after_the_recipe_roles() -> None:
    config = DataLakeSourceConfig.model_validate(
        _config(
            aws_config={**AWS_CONFIG, "aws_role": "arn:aws:iam::999999999999:role/Hub"},
            aws_accounts=[{"aws_role": {"RoleArn": ROLE_A, "ExternalId": "ext"}}],
        )
    )

    account_aws = config.aws_config_for_account(config.aws_accounts[0])

    assert [role.RoleArn for role in account_aws._normalized_aws_roles()] == [
        "arn:aws:iam::999999999999:role/Hub",
        ROLE_A,
    ]
    assert account_aws._normalized_aws_roles()[1].ExternalId == "ext"
    assert account_aws.aws_region == AWS_CONFIG["aws_region"]
    assert config.aws_config is not None
    assert config.aws_config.aws_role == "arn:aws:iam::999999999999:role/Hub"


def test_platform_instance_defaults_to_the_role_account() -> None:
    config = DataLakeSourceConfig.model_validate(
        _config(
            aws_accounts=[
                {"aws_role": ROLE_A},
                {"aws_role": AwsAssumeRoleConfig(RoleArn=ROLE_B)},
                {"aws_role": ROLE_A, "platform_instance": "prod-data"},
            ]
        )
    )

    instances: List[Any] = [a.platform_instance for a in config.aws_accounts]
    assert instances == [ACCOUNT_A, ACCOUNT_B, "prod-data"]


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"platform_instance": "shared"}, id="top-level-instance"),
        pytest.param(
            {"aws_accounts": [{"aws_role": ROLE_A}, {"aws_role": ROLE_A}]},
            id="duplicate-instance",
        ),
        pytest.param(
            {"aws_accounts": [{"aws_role": "not-an-arn"}]},
            id="no-account-in-role",
        ),
        pytest.param(
            {"path_specs": [{"include": "/data/*.csv"}]},
            id="local-paths",
        ),
        pytest.param(
            {
                "aws_config": {
                    **AWS_CONFIG,
                    "aws_access_key_id": "key",
                    "aws_secret_access_key": "secret",
                }
            },
            id="static-keys",
        ),
        pytest.param(
            {"aws_config": {**AWS_CONFIG, "aws_profile": "hub"}},
            id="named-profile",
        ),
    ],
)
def test_invalid_multi_account_config_is_rejected(overrides: Dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        DataLakeSourceConfig.model_validate(_config(**overrides))


def test_unique_instances_are_accepted() -> None:
    config = DataLakeSourceConfig.model_validate(_config())
    instances: Set[Any] = {a.platform_instance for a in config.aws_accounts}
    assert instances == {ACCOUNT_A, ACCOUNT_B}


def test_probe_refuses_multi_account_recipes() -> None:
    multi = DataLakeSourceConfig.model_validate(_config())
    single = DataLakeSourceConfig.model_validate(_config(aws_accounts=[]))

    assert s3_probe_refusal(multi) is not None
    assert s3_probe_refusal(single) is None
