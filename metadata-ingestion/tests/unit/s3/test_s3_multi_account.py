from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from unittest.mock import patch

import pytest
from boto3.session import Session
from botocore.exceptions import ClientError
from pydantic import ValidationError

from datahub.emitter.mce_builder import (
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.aws import aws_common
from datahub.ingestion.source.aws.aws_common import (
    AwsAssumeRoleConfig,
    AwsConnectionConfig,
    AwsEnvironment,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.s3.config import DataLakeSourceConfig
from datahub.ingestion.source.s3.s3_probe import s3_probe_refusal
from datahub.ingestion.source.s3.source import S3Source
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
)

INSTANCE_A = make_dataplatform_instance_urn("s3", "111111111111")
INSTANCE_B = make_dataplatform_instance_urn("s3", "222222222222")

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


def _final_aspects(wus: Iterable[MetadataWorkUnit]) -> Dict[str, Dict[str, Any]]:
    """The last value of each aspect per urn, as DataHub would store it."""
    aspects_by_urn: Dict[str, Dict[str, Any]] = {}
    for wu in wus:
        # Patches (e.g. lastModified) are not full aspect values.
        if not isinstance(wu.metadata, MetadataChangeProposalWrapper):
            continue
        assert wu.metadata.entityUrn and wu.metadata.aspect
        aspects_by_urn.setdefault(wu.metadata.entityUrn, {})[
            type(wu.metadata.aspect).__name__
        ] = wu.metadata.aspect
    return aspects_by_urn


def _containers_by_name(
    aspects_by_urn: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    return {
        aspects["ContainerPropertiesClass"].name: aspects
        for aspects in aspects_by_urn.values()
        if isinstance(aspects.get("ContainerPropertiesClass"), ContainerPropertiesClass)
    }


def _instance(aspects: Dict[str, Any]) -> Optional[str]:
    platform_instance = aspects["DataPlatformInstanceClass"]
    assert isinstance(platform_instance, DataPlatformInstanceClass)
    return platform_instance.instance


def _browse_path(aspects: Dict[str, Any]) -> List[Optional[str]]:
    browse_path = aspects["BrowsePathsV2Class"]
    assert isinstance(browse_path, BrowsePathsV2Class)
    return [entry.urn for entry in browse_path.path]


def _last_role(aws_config: AwsConnectionConfig) -> str:
    assert isinstance(aws_config.aws_role, list)
    role = aws_config.aws_role[-1]
    assert isinstance(role, AwsAssumeRoleConfig)
    return role.RoleArn


def test_each_account_is_ingested_under_its_own_role_and_instance(
    s3: Session,
) -> None:
    _seed_account(s3, ROLE_A, "bucket-a", ["data/orders/file.csv"])
    _seed_account(s3, ROLE_B, "bucket-b", ["data/users/file.csv"])
    _seed_account(s3, ROLE_B, "empty-bucket", [])
    source = S3Source.create(
        _config(
            path_specs=[
                *FOLDERS_ONLY_SPECS,
                {"include": "s3://*/data/{table}/*.csv"},
            ],
            use_s3_bucket_tags=True,
            profiling={"enabled": True},
        ),
        PipelineContext(run_id="multi-account"),
    )
    # moto only scopes ListBuckets by account, so reads made with the wrong
    # credentials would still succeed; record the role each read used instead.
    reads: List[Tuple[str, str]] = []

    def record_tags(
        bucket: str, key_prefix: Any, urn: str, aws_config: Any, *_: Any
    ) -> None:
        reads.append((f"tags:{bucket}", _last_role(aws_config)))

    def record_profile(
        table_data: Any, dataset_urn: str, *_: Any, **__: Any
    ) -> Iterable[Any]:
        reads.append((f"profile:{dataset_urn}", _last_role(source.profiler.aws_config)))
        return iter([])

    with (
        patch("datahub.ingestion.source.s3.source.get_s3_tags", record_tags),
        patch.object(source.profiler, "get_table_profile", record_profile),
    ):
        aspects = _final_aspects(source.get_workunits())

    dataset_a = make_dataset_urn_with_platform_instance(
        "s3", "bucket-a/data/orders", ACCOUNT_A
    )
    dataset_b = make_dataset_urn_with_platform_instance(
        "s3", "bucket-b/data/users", ACCOUNT_B
    )
    containers = _containers_by_name(aspects)
    assert set(containers) == {"bucket-a", "bucket-b", "empty-bucket", "data"}
    assert _instance(aspects[dataset_a]) == INSTANCE_A
    assert _instance(aspects[dataset_b]) == INSTANCE_B
    assert _instance(containers["empty-bucket"]) == INSTANCE_B

    # The account is the first level of the final browse path for buckets, and
    # folders and datasets inherit it through their container.
    assert _browse_path(containers["bucket-a"]) == [INSTANCE_A]
    assert _browse_path(containers["empty-bucket"]) == [INSTANCE_B]
    bucket_a_urn, data_a_urn = _browse_path(aspects[dataset_a])[1:]
    assert _browse_path(aspects[dataset_a]) == [INSTANCE_A, bucket_a_urn, data_a_urn]
    assert data_a_urn is not None
    assert _browse_path(aspects[data_a_urn]) == [INSTANCE_A, bucket_a_urn]
    assert _browse_path(aspects[dataset_b])[0] == INSTANCE_B

    assert sorted(reads) == sorted(
        [
            ("tags:bucket-a", ROLE_A),
            (f"profile:{dataset_a}", ROLE_A),
            ("tags:bucket-b", ROLE_B),
            (f"profile:{dataset_b}", ROLE_B),
        ]
    )
    assert source.report.aws_accounts_ingested == 2
    assert not source.report.failures
    assert source.source_config.platform_instance is None
    assert source.profiler.aws_config is source.source_config.aws_config


def test_denied_role_fails_only_that_account(s3: Session) -> None:
    _seed_account(s3, ROLE_A, "bucket-a", ["raw/file.csv"])
    _seed_account(s3, ROLE_B, "bucket-b", ["curated/file.csv"])
    source = S3Source.create(_config(), PipelineContext(run_id="multi-account"))
    recipe_container_wu_creator = source.container_WU_creator
    assume_role = aws_common.assume_role

    def deny_account_a(role: AwsAssumeRoleConfig, **kwargs: Any) -> dict:
        if role.RoleArn == ROLE_A:
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                "AssumeRole",
            )
        return assume_role(role=role, **kwargs)

    with patch.object(aws_common, "assume_role", deny_account_a):
        containers = _containers_by_name(_final_aspects(source.get_workunits()))

    assert set(containers) == {"bucket-b", "curated"}
    assert source.report.aws_accounts_ingested == 1
    assert source.report.aws_accounts_failed == [ACCOUNT_A]
    # A reported failure is what keeps stale-entity removal from soft-deleting
    # metadata of the account that could not be read.
    assert [f.title for f in source.report.failures] == [
        "Failed to read an AWS account"
    ]
    assert source.source_config.platform_instance is None
    assert source.container_WU_creator is recipe_container_wu_creator


def test_failing_path_spec_does_not_skip_the_accounts_other_specs(s3: Session) -> None:
    _seed_account(s3, ROLE_A, "bucket-a", ["raw/file.csv"])
    source = S3Source.create(
        _config(aws_accounts=[{"aws_role": ROLE_A}]),
        PipelineContext(run_id="multi-account"),
    )
    process_folders = source._process_folders

    def fail_bucket_spec(path_spec: PathSpec) -> Iterable[MetadataWorkUnit]:
        if path_spec.include == "s3://*/":
            raise RuntimeError("boom")
        return process_folders(path_spec)

    with patch.object(source, "_process_folders", side_effect=fail_bucket_spec):
        containers = _containers_by_name(
            _final_aspects(source.get_workunits_internal())
        )

    assert set(containers) == {"bucket-a", "raw"}
    assert source.report.aws_accounts_failed == [ACCOUNT_A]
    assert [f.title for f in source.report.failures] == [
        "Unexpected error while ingesting an AWS account"
    ]


def test_account_that_matches_nothing_is_warned_about(s3: Session) -> None:
    _seed_account(s3, ROLE_A, "bucket-a", ["raw/file.csv"])
    source = S3Source.create(_config(), PipelineContext(run_id="multi-account"))

    list(source.get_workunits_internal())

    assert source.report.aws_accounts_ingested == 2
    assert [w.title for w in source.report.warnings] == [
        "AWS account produced no metadata"
    ]
    assert ACCOUNT_B in str(list(source.report.warnings))


def test_account_role_is_chained_after_the_recipe_roles() -> None:
    hub = "arn:aws:iam::999999999999:role/Hub"
    config = DataLakeSourceConfig.model_validate(
        _config(
            aws_config={**AWS_CONFIG, "aws_role": hub},
            aws_accounts=[{"aws_role": {"RoleArn": ROLE_A, "ExternalId": "ext"}}],
        )
    )

    account_aws = config.aws_config_for_account(config.aws_accounts[0])

    assert account_aws.aws_role == [
        AwsAssumeRoleConfig(RoleArn=hub),
        AwsAssumeRoleConfig(RoleArn=ROLE_A, ExternalId="ext"),
    ]
    assert account_aws.aws_region == AWS_CONFIG["aws_region"]
    assert config.aws_config is not None
    assert config.aws_config.aws_role == hub


def test_platform_instance_defaults_to_the_role_account() -> None:
    config = DataLakeSourceConfig.model_validate(
        _config(
            aws_accounts=[
                {"aws_role": ROLE_A},
                {"aws_role": AwsAssumeRoleConfig(RoleArn=ROLE_B)},
                {"aws_role": "arn:aws-us-gov:iam::333333333333:role/team/Reader"},
                {"aws_role": ROLE_A, "platform_instance": "prod-data"},
            ]
        )
    )

    assert [a.resolved_platform_instance for a in config.aws_accounts] == [
        ACCOUNT_A,
        ACCOUNT_B,
        "333333333333",
        "prod-data",
    ]


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
            {"aws_accounts": [{"aws_role": ROLE_A, "platform_instance": ""}]},
            id="empty-instance",
        ),
        pytest.param(
            {
                "path_specs": [
                    {"include": "s3://shared-lake/*/", "emit_folders_only": True}
                ]
            },
            id="named-bucket",
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


def test_probe_refuses_multi_account_recipes() -> None:
    multi = DataLakeSourceConfig.model_validate(_config())
    single = DataLakeSourceConfig.model_validate(_config(aws_accounts=[]))

    assert s3_probe_refusal(multi) is not None
    assert s3_probe_refusal(single) is None
