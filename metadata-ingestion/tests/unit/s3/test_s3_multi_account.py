from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
from boto3.session import Session
from botocore.exceptions import ClientError
from pydantic import ValidationError

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import AwsAccountKey, BucketKey
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
    ContainerClass,
    ContainerPropertiesClass,
    SubTypesClass,
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


def _account_urn(account_id: str) -> str:
    return AwsAccountKey(
        platform="s3",
        env="PROD",
        backcompat_env_as_instance=True,
        account_id=account_id,
    ).as_urn()


def _bucket_urn(bucket: str) -> str:
    # What a single-account recipe produces for the same bucket.
    return BucketKey(
        platform="s3", env="PROD", backcompat_env_as_instance=True, bucket_name=bucket
    ).as_urn()


def _parent(aspects: Dict[str, Any]) -> str:
    container = aspects["ContainerClass"]
    assert isinstance(container, ContainerClass)
    return container.container


def _browse_path(aspects: Dict[str, Any]) -> List[Optional[str]]:
    browse_path = aspects["BrowsePathsV2Class"]
    assert isinstance(browse_path, BrowsePathsV2Class)
    return [entry.urn for entry in browse_path.path]


def _last_role(aws_config: AwsConnectionConfig) -> str:
    assert isinstance(aws_config.aws_role, list)
    role = aws_config.aws_role[-1]
    assert isinstance(role, AwsAssumeRoleConfig)
    return role.RoleArn


def test_each_account_is_ingested_under_its_own_role_and_container(
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
            aws_accounts=[
                {"aws_role": ROLE_A, "name": "analytics-prod"},
                {"aws_role": ROLE_B},
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

    # Bucket names are globally unique, so no URN carries the account: datasets
    # and buckets match what a single-account recipe (and Glue lineage) uses.
    dataset_a = make_dataset_urn("s3", "bucket-a/data/orders")
    dataset_b = make_dataset_urn("s3", "bucket-b/data/users")
    account_a, account_b = _account_urn(ACCOUNT_A), _account_urn(ACCOUNT_B)
    bucket_a = _bucket_urn("bucket-a")
    containers = _containers_by_name(aspects)
    assert set(containers) == {
        "analytics-prod",
        ACCOUNT_B,
        "bucket-a",
        "bucket-b",
        "empty-bucket",
        "data",
    }
    assert (
        containers["analytics-prod"]["ContainerPropertiesClass"].customProperties[
            "account_id"
        ]
        == ACCOUNT_A
    )
    account_subtypes = containers[ACCOUNT_B]["SubTypesClass"]
    assert isinstance(account_subtypes, SubTypesClass)
    assert account_subtypes.typeNames == ["AWS Account"]

    # Each bucket sits under the account whose role listed it.
    assert _parent(aspects[bucket_a]) == account_a
    assert _parent(aspects[_bucket_urn("bucket-b")]) == account_b
    assert _parent(aspects[_bucket_urn("empty-bucket")]) == account_b

    # The account is the first level of the final browse path for buckets, and
    # folders and datasets inherit it through their container.
    assert _browse_path(aspects[account_a]) == []
    assert _browse_path(aspects[bucket_a]) == [account_a]
    data_a = _parent(aspects[dataset_a])
    assert _browse_path(aspects[data_a]) == [account_a, bucket_a]
    assert _browse_path(aspects[dataset_a]) == [account_a, bucket_a, data_a]
    assert _browse_path(aspects[dataset_b])[0] == account_b

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
    assert source.source_config.aws_accounts
    assert source.profiler.aws_config is source.source_config.aws_config


def test_denied_role_fails_only_that_account(s3: Session) -> None:
    _seed_account(s3, ROLE_A, "bucket-a", ["raw/file.csv"])
    _seed_account(s3, ROLE_B, "bucket-b", ["curated/file.csv"])
    graph = MagicMock()
    graph.get_latest_timeseries_value.return_value = None  # no previous checkpoint
    source = S3Source.create(
        _config(stateful_ingestion={"enabled": True}),
        PipelineContext(
            run_id="multi-account", pipeline_name="multi-account", graph=graph
        ),
    )
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

    # No container is created for the account whose role was denied.
    assert set(containers) == {ACCOUNT_B, "bucket-b", "curated"}
    assert source.report.aws_accounts_ingested == 1
    assert source.report.aws_accounts_failed == [ACCOUNT_A]
    assert source.report.stale_removal_skipped_due_to_account_failures
    # A reported failure is what keeps stale-entity removal from soft-deleting
    # metadata of the account that could not be read.
    assert [f.title for f in source.report.failures] == [
        "Failed to read an AWS account"
    ]
    assert source.source_config.aws_accounts
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

    assert set(containers) == {ACCOUNT_A, "bucket-a", "raw"}
    assert source.report.aws_accounts_failed == [ACCOUNT_A]
    assert [f.title for f in source.report.failures] == [
        "Unexpected error while ingesting an AWS account"
    ]


def test_account_that_matches_nothing_is_warned_about(s3: Session) -> None:
    _seed_account(s3, ROLE_A, "bucket-a", ["raw/file.csv"])
    source = S3Source.create(_config(), PipelineContext(run_id="multi-account"))

    containers = _containers_by_name(_final_aspects(source.get_workunits_internal()))

    assert ACCOUNT_B not in containers
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


def test_account_id_is_read_from_the_role_arn() -> None:
    config = DataLakeSourceConfig.model_validate(
        _config(
            aws_accounts=[
                {"aws_role": ROLE_A},
                {"aws_role": AwsAssumeRoleConfig(RoleArn=ROLE_B)},
                {"aws_role": "arn:aws-us-gov:iam::333333333333:role/team/Reader"},
            ]
        )
    )

    assert [a.account_id for a in config.aws_accounts] == [
        ACCOUNT_A,
        ACCOUNT_B,
        "333333333333",
    ]


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param(
            {"aws_accounts": [{"aws_role": ROLE_A}, {"aws_role": ROLE_A}]},
            id="duplicate-account",
        ),
        pytest.param(
            {"aws_accounts": [{"aws_role": "not-an-arn"}]},
            id="no-account-in-role",
        ),
        pytest.param(
            {"aws_accounts": [{"aws_role": ROLE_A, "name": ""}]},
            id="empty-name",
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


def test_placeholder_in_the_bucket_segment_counts_as_a_wildcard() -> None:
    # {table} in the bucket segment lists every bucket, like s3://*/.
    config = DataLakeSourceConfig.model_validate(
        _config(path_specs=[{"include": "s3://{table}/*.csv"}])
    )

    assert config.aws_accounts


def test_probe_refuses_multi_account_recipes() -> None:
    multi = DataLakeSourceConfig.model_validate(_config())
    single = DataLakeSourceConfig.model_validate(_config(aws_accounts=[]))

    assert s3_probe_refusal(multi) is not None
    assert s3_probe_refusal(single) is None
