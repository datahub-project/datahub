import json
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Tuple, Type
from unittest.mock import MagicMock

import boto3
import botocore
import pytest
from botocore.stub import Stubber
from packaging.version import Version

from datahub.configuration.common import GraphError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.aws_messaging import s3_notifications
from datahub.ingestion.source.aws_messaging.base import AwsMessagingSourceBase
from datahub.ingestion.source.aws_messaging.clients import S3NotificationClient
from datahub.ingestion.source.aws_messaging.constants import (
    MIN_BOTOCORE_FOR_BUCKET_REGION,
    S3_SOURCE_COMPRESSIONS,
    S3_SOURCE_FILE_TYPES,
)
from datahub.ingestion.source.aws_messaging.models import Arn, BucketNotification
from datahub.ingestion.source.aws_messaging.report import SqsSourceReport
from datahub.ingestion.source.aws_messaging.resolver import ResourceResolver
from datahub.ingestion.source.aws_messaging.sns_source import SnsSource
from datahub.ingestion.source.aws_messaging.sqs_source import SqsSource
from datahub.ingestion.source.data_lake_common.path_spec import (
    SUPPORTED_COMPRESSIONS,
    SUPPORTED_FILE_TYPES,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    MetadataChangeProposalClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.utilities.urns.dataset_urn import DatasetUrn
from tests.unit.aws_messaging.conftest import (
    ACCOUNT,
    REGION,
    base_config,
    sns_client,
    sqs_client,
)

if TYPE_CHECKING:
    # Type stubs only; not installed at runtime.
    from mypy_boto3_s3.type_defs import NotificationConfigurationTypeDef

QUEUE_URN = f"urn:li:dataset:(urn:li:dataPlatform:sqs,{ACCOUNT}.{REGION}.uploads,PROD)"
TOPIC_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:sns,{ACCOUNT}.{REGION}.uploads-topic,PROD)"
)
# Named the way the s3 source names them, after a path_spec's {table} folder:
# s3://landing/incoming/{table}/*.parquet
# A path_spec of s3://landing/{table}/*.parquet names the folder itself.
FOLDER_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing/incoming,PROD)"
SIBLING_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing/incoming2/orders,PROD)"
ORDERS_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing/incoming/orders,PROD)"
REFUNDS_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing/incoming/refunds,PROD)"
ARCHIVE_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing/archive/orders,PROD)"
OTHER_BUCKET_URN = "urn:li:dataset:(urn:li:dataPlatform:s3,landing-old/orders,PROD)"
S3_DATASETS = [
    FOLDER_URN,
    SIBLING_URN,
    ORDERS_URN,
    REFUNDS_URN,
    ARCHIVE_URN,
    OTHER_BUCKET_URN,
]


def _queue_arn(name: str) -> str:
    url = sqs_client().create_queue(QueueName=name)["QueueUrl"]
    return sqs_client().get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]


def _bucket(notifications: Optional["NotificationConfigurationTypeDef"] = None) -> None:
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket="landing")
    if notifications is not None:
        s3.put_bucket_notification_configuration(
            Bucket="landing",
            NotificationConfiguration=notifications,
            SkipDestinationValidation=True,
        )


def _seed() -> None:
    queue_arn = _queue_arn("uploads")
    topic_arn = sns_client().create_topic(Name="uploads-topic")["TopicArn"]
    _bucket(
        {
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [{"Name": "prefix", "Value": "incoming/"}]
                        }
                    },
                }
            ],
            "TopicConfigurations": [
                {"TopicArn": topic_arn, "Events": ["s3:ObjectRemoved:*"]}
            ],
        }
    )


def upstreams(source: AwsMessagingSourceBase) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = {}
    for workunit in source.get_workunits():
        metadata = workunit.metadata
        if (
            isinstance(metadata, MetadataChangeProposalClass)
            and metadata.aspectName == "upstreamLineage"
        ):
            assert metadata.entityUrn is not None and metadata.aspect is not None
            result[metadata.entityUrn] = {
                op["value"]["dataset"] for op in json.loads(metadata.aspect.value)
            }
    return result


def _hit(
    urn: str,
    prefix: str,
    platform: str,
    platform_instance: Optional[str],
    env: Optional[str],
) -> bool:
    dataset = DatasetUrn.from_string(urn)
    return (
        urn.startswith(prefix)
        and dataset.platform == f"urn:li:dataPlatform:{platform}"
        and (env is None or dataset.env == env)
        and (
            platform_instance is None
            or dataset.name.startswith(f"{platform_instance}.")
        )
    )


def search_graph(
    existing: List[str], prior_upstreams: Optional[List[UpstreamClass]] = None
) -> MagicMock:
    """A graph whose URN-prefix search runs over `existing`, honouring the
    platform, platform_instance and env filters, and whose queue already has
    `prior_upstreams` from an earlier run."""
    graph = MagicMock()

    def get_urns_by_filter(**kwargs: Any) -> Iterator[str]:
        prefix = kwargs["extraFilters"][0]["values"][0]
        return iter(
            urn
            for urn in existing
            if _hit(
                urn,
                prefix,
                kwargs["platform"],
                kwargs.get("platform_instance"),
                kwargs.get("env"),
            )
        )

    graph.get_urns_by_filter.side_effect = get_urns_by_filter
    graph.get_related_entities.return_value = []
    graph.get_aspect.side_effect = lambda urn, aspect: (
        UpstreamLineageClass(upstreams=prior_upstreams)
        if prior_upstreams and urn == QUEUE_URN
        else None
    )
    graph.get_schema_metadata.return_value = None
    return graph


def _source(
    source_class: Type[AwsMessagingSourceBase], existing: List[str], **lineage: Any
) -> AwsMessagingSourceBase:
    graph = search_graph(existing)
    return source_class.create(
        base_config(lineage={"lambda_functions": False, **lineage}),
        PipelineContext(run_id="s3-test", graph=graph),
    )


def test_each_source_links_its_own_targets(aws: None) -> None:
    _seed()
    # The queue's notification is on `incoming/`, so the `landing/incoming`
    # folder and every table folder under it are upstreams, but not
    # `incoming2/`. The topic's has no prefix, so the whole bucket is, but not a
    # bucket whose name merely starts the same.
    assert upstreams(_source(SqsSource, S3_DATASETS, s3_notifications=True)) == {
        QUEUE_URN: {FOLDER_URN, ORDERS_URN, REFUNDS_URN}
    }
    assert upstreams(_source(SnsSource, S3_DATASETS, s3_notifications=True)) == {
        TOPIC_URN: {FOLDER_URN, SIBLING_URN, ORDERS_URN, REFUNDS_URN, ARCHIVE_URN}
    }


def test_no_edge_without_a_matching_s3_dataset(aws: None) -> None:
    _seed()
    source = _source(SqsSource, [], s3_notifications=True)
    assert upstreams(source) == {}
    assert len(source.base_report.lineage_edges_skipped) == 1


def test_off_by_default(aws: None) -> None:
    _seed()
    source = _source(SqsSource, S3_DATASETS)
    assert upstreams(source) == {}
    assert not source.base_report.lineage_edges_skipped


def test_lowercase_matches_the_s3_source_setting(aws: None) -> None:
    queue_arn = _queue_arn("uploads")
    _bucket(
        {
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [{"Name": "prefix", "Value": "Incoming/"}]
                        }
                    },
                }
            ]
        }
    )
    graph = search_graph(S3_DATASETS)

    def source(lowercase: bool) -> AwsMessagingSourceBase:
        return SqsSource.create(
            base_config(
                lineage={"lambda_functions": False, "s3_notifications": True},
                platform_map={"s3": {"convert_urns_to_lowercase": lowercase}},
            ),
            PipelineContext(run_id="s3-test", graph=graph),
        )

    # The S3 source lowercased `landing/Incoming/...` to `landing/incoming/...`.
    assert upstreams(source(lowercase=True)) == {
        QUEUE_URN: {FOLDER_URN, ORDERS_URN, REFUNDS_URN}
    }
    assert upstreams(source(lowercase=False)) == {}


@pytest.mark.skipif(
    Version(botocore.__version__) < Version(MIN_BOTOCORE_FOR_BUCKET_REGION),
    reason="ListBuckets has no BucketRegion parameter in this botocore",
)
def test_bucket_listing_is_filtered_to_the_recipe_region() -> None:
    client = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    with Stubber(client) as stubber:
        stubber.add_response(
            "list_buckets",
            {
                "Buckets": [{"Name": "landing", "BucketRegion": REGION}],
                "ContinuationToken": "page-2",
            },
            {"BucketRegion": REGION, "MaxBuckets": 1000},
        )
        stubber.add_response(
            "list_buckets",
            {"Buckets": [{"Name": "elsewhere", "BucketRegion": "eu-west-1"}]},
            {"BucketRegion": REGION, "MaxBuckets": 1000, "ContinuationToken": "page-2"},
        )
        names = list(
            S3NotificationClient(client, SqsSourceReport()).list_bucket_names(
                REGION, on_error=lambda _error: None
            )
        )
    assert names == ["landing"]


def test_old_botocore_lists_without_the_region_filter() -> None:
    # Before botocore 1.35.42, ListBuckets has no BucketRegion parameter; sending it
    # would fail validation and look like a denied listing.
    client = MagicMock()
    client.meta.service_model.operation_model.return_value.input_shape.members = {
        "MaxBuckets": None,
        "ContinuationToken": None,
    }
    client.get_paginator.return_value.paginate.return_value = [
        {"Buckets": [{"Name": "landing"}, {"Name": "elsewhere"}]}
    ]
    report = SqsSourceReport()
    names = list(
        S3NotificationClient(client, report).list_bucket_names(
            REGION, on_error=lambda _error: None
        )
    )
    assert names == ["landing", "elsewhere"]
    client.get_paginator.return_value.paginate.assert_called_once_with(
        PaginationConfig={"PageSize": 1000}
    )
    assert [w.title for w in report.warnings] == ["Bucket region filter unavailable"]
    assert not report.failures


def upstream_ops(source: AwsMessagingSourceBase) -> List[Tuple[str, str]]:
    return [
        (op["op"], op["path"])
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "upstreamLineage"
        and wu.metadata.aspect is not None
        for op in json.loads(wu.metadata.aspect.value)
    ]


def _edge(dataset: str, relationship: str) -> UpstreamClass:
    return UpstreamClass(
        dataset=dataset,
        type=DatasetLineageTypeClass.TRANSFORMED,
        properties={"relationship": relationship},
    )


def _removals(existing: List[str]) -> List[Tuple[str, str]]:
    graph = search_graph(
        existing,
        prior_upstreams=[
            _edge(ORDERS_URN, "s3_notification"),
            _edge(TOPIC_URN, "subscription"),
        ],
    )
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False, "s3_notifications": True}),
        PipelineContext(run_id="s3-test", graph=graph),
    )
    return [op for op in upstream_ops(source) if op[0] == "remove"]


def test_removed_notification_retracts_the_s3_edge(aws: None) -> None:
    sqs_client().create_queue(QueueName="uploads")
    _bucket()
    # The sns source's subscription edge is left alone.
    assert _removals(S3_DATASETS) == [
        ("remove", "/upstreams/" + ORDERS_URN.replace("/", "~1"))
    ]


def test_unreadable_notifications_remove_nothing(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed()
    monkeypatch.setattr(
        S3NotificationClient, "get_notifications", lambda self, bucket: None
    )
    assert _removals(S3_DATASETS) == []


def test_no_dataset_found_removes_nothing(aws: None) -> None:
    # The notification is still there; an empty search is index lag or a
    # platform_map mismatch, not a removed notification.
    _seed()
    assert _removals([]) == []


def test_cap_links_the_first_datasets_by_name(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Every match is read and the cap applied by name, so each run links the same
    # datasets and the earlier orders edge past the cap is removed.
    _seed()
    monkeypatch.setattr(s3_notifications, "MAX_S3_DATASETS_PER_NOTIFICATION", 1)
    assert _removals(S3_DATASETS) == [
        ("remove", "/upstreams/" + ORDERS_URN.replace("/", "~1"))
    ]


def test_failed_dataset_search_removes_nothing(aws: None) -> None:
    _seed()
    graph = search_graph(
        S3_DATASETS, prior_upstreams=[_edge(ORDERS_URN, "s3_notification")]
    )
    graph.get_urns_by_filter.side_effect = GraphError("search timed out")
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False, "s3_notifications": True}),
        PipelineContext(run_id="s3-test", graph=graph),
    )
    assert [op for op in upstream_ops(source) if op[0] == "remove"] == []
    assert "S3 dataset search failed" in [w.title for w in source.get_report().warnings]


def test_search_filters_on_the_upper_case_env(aws: None) -> None:
    # Dataset URNs carry DEV even when the recipe says dev, and the search
    # filter is an exact match; the platform_map validator upper-cases it.
    _seed()
    graph = search_graph(S3_DATASETS)
    source = SqsSource.create(
        base_config(
            lineage={"lambda_functions": False, "s3_notifications": True},
            platform_map={"s3": {"env": "dev"}},
        ),
        PipelineContext(run_id="s3-test", graph=graph),
    )
    list(source.get_workunits())
    assert {call.kwargs["env"] for call in graph.get_urns_by_filter.call_args_list} == {
        "DEV"
    }


def test_unresolved_own_target_removes_nothing(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The notification's queue is this source's own, but this run couldn't
    # resolve it, so its earlier S3 edges are unknown rather than gone.
    _seed()
    original = ResourceResolver.urn

    def unresolved(self: ResourceResolver, arn: Optional[Arn]) -> Optional[str]:
        if arn is not None and arn.resource_name == "uploads":
            return None
        return original(self, arn)

    monkeypatch.setattr(ResourceResolver, "urn", unresolved)
    assert _removals(S3_DATASETS) == []


def _prior_s3_removals(prior: List[str], **config: Any) -> List[Tuple[str, str]]:
    graph = search_graph(
        S3_DATASETS, prior_upstreams=[_edge(urn, "s3_notification") for urn in prior]
    )
    source = SqsSource.create(
        base_config(
            lineage={"lambda_functions": False, "s3_notifications": True}, **config
        ),
        PipelineContext(run_id="s3-test", graph=graph),
    )
    return [op for op in upstream_ops(source) if op[0] == "remove"]


def test_edge_from_a_bucket_this_run_did_not_list_is_kept(aws: None) -> None:
    # A bucket in another account can notify this queue, but its notifications
    # can't be read from here.
    sqs_client().create_queue(QueueName="uploads")
    _bucket()
    foreign = "urn:li:dataset:(urn:li:dataPlatform:s3,partner-bucket/orders,PROD)"
    assert _prior_s3_removals([ORDERS_URN, foreign]) == [
        ("remove", "/upstreams/" + ORDERS_URN.replace("/", "~1"))
    ]


def test_notification_to_a_filtered_out_queue_still_retracts(aws: None) -> None:
    # The bucket notifies a queue the pattern excludes; that queue is never this
    # source's, so S3 edges into the queues it does ingest are still checked.
    sqs_client().create_queue(QueueName="uploads")
    tmp_arn = _queue_arn("uploads-tmp")
    _bucket(
        {
            "QueueConfigurations": [
                {"QueueArn": tmp_arn, "Events": ["s3:ObjectCreated:*"]}
            ]
        }
    )
    assert _prior_s3_removals([ORDERS_URN], queue_pattern={"deny": [".*-tmp$"]}) == [
        ("remove", "/upstreams/" + ORDERS_URN.replace("/", "~1"))
    ]


def test_unmatched_notification_keeps_only_its_own_targets_edges(aws: None) -> None:
    # The archive queue's notification matches no S3 dataset, so its earlier S3
    # edges are unknown; the uploads queue's removed notification still retracts.
    _queue_arn("uploads")
    archive_arn = _queue_arn("archive")
    _bucket(
        {
            "QueueConfigurations": [
                {
                    "QueueArn": archive_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [{"Name": "prefix", "Value": "nothing/"}]
                        }
                    },
                }
            ]
        }
    )
    assert _prior_s3_removals([ORDERS_URN]) == [
        ("remove", "/upstreams/" + ORDERS_URN.replace("/", "~1"))
    ]


def test_unresolved_own_target_keeps_only_its_own_edges(
    aws: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The archive queue this run couldn't resolve keeps its earlier S3 edges, but
    # the uploads queue's removed notification still retracts.
    _queue_arn("uploads")
    _bucket(
        {
            "QueueConfigurations": [
                {"QueueArn": _queue_arn("archive"), "Events": ["s3:ObjectCreated:*"]}
            ]
        }
    )
    original = ResourceResolver.urn

    def unresolved(self: ResourceResolver, arn: Optional[Arn]) -> Optional[str]:
        if arn is not None and arn.resource_name == "archive":
            return None
        return original(self, arn)

    monkeypatch.setattr(ResourceResolver, "urn", unresolved)
    assert _prior_s3_removals([ORDERS_URN]) == [
        ("remove", "/upstreams/" + ORDERS_URN.replace("/", "~1"))
    ]


def test_unreadable_lineage_keeps_s3_edges(aws: None) -> None:
    sqs_client().create_queue(QueueName="uploads")
    _bucket()
    graph = search_graph(S3_DATASETS)
    graph.get_aspect.side_effect = GraphError("GMS unavailable")
    source = SqsSource.create(
        base_config(lineage={"lambda_functions": False, "s3_notifications": True}),
        PipelineContext(run_id="s3-test", graph=graph),
    )
    assert [op for op in upstream_ops(source) if op[0] == "remove"] == []
    assert "Stale lineage not checked" in [
        w.title for w in source.get_report().warnings
    ]


def test_platform_map_sets_instance_and_env(aws: None) -> None:
    _seed()
    lake_orders = (
        "urn:li:dataset:(urn:li:dataPlatform:s3,lake.landing/incoming/orders,DEV)"
    )
    existing = [lake_orders, ORDERS_URN]

    def queue_upstreams(**config: Any) -> Set[str]:
        source = SqsSource.create(
            base_config(
                lineage={"lambda_functions": False, "s3_notifications": True},
                **config,
            ),
            PipelineContext(run_id="s3-test", graph=search_graph(existing)),
        )
        return upstreams(source).get(QUEUE_URN, set())

    assert queue_upstreams(
        platform_map={"s3": {"platform_instance": "lake", "env": "dev"}}
    ) == {lake_orders}
    assert queue_upstreams() == {ORDERS_URN}


def _s3_urn(name: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{name},PROD)"


@pytest.mark.parametrize(
    "prefix, suffix, matched",
    [
        # A plain prefix matches like S3 does, so `incoming2/` matches too.
        (
            "incoming",
            None,
            {
                "landing/incoming",
                "landing/incoming/orders",
                "landing/incoming/orders.v2",
                "landing/incoming2/orders",
                "landing/incoming/a.csv",
                "landing/incoming/b.json.gz",
            },
        ),
        # A leading slash is part of the key, so it matches no dataset.
        ("/incoming/", None, set()),
        # A suffix filters file datasets; folder datasets, dotted ones too, are
        # kept.
        (
            "incoming/",
            ".csv",
            {
                "landing/incoming",
                "landing/incoming/orders",
                "landing/incoming/orders.v2",
                "landing/incoming/a.csv",
            },
        ),
        # A suffix with no prefix applies across the whole bucket.
        (
            None,
            ".csv",
            {
                "landing/incoming",
                "landing/incoming/orders",
                "landing/incoming/orders.v2",
                "landing/incoming2/orders",
                "landing/incoming/a.csv",
            },
        ),
    ],
    ids=["plain-prefix", "leading-slash", "prefix-and-suffix", "suffix-only"],
)
def test_notification_filters_match_datasets(
    prefix: Optional[str], suffix: Optional[str], matched: Set[str]
) -> None:
    names = [
        "landing/incoming",
        "landing/incoming/orders",
        "landing/incoming/orders.v2",
        "landing/incoming2/orders",
        "landing/incoming/a.csv",
        "landing/incoming/b.json.gz",
    ]
    graph = search_graph([_s3_urn(name) for name in names])
    lineage = s3_notifications.S3NotificationLineage(
        client=MagicMock(),
        resolver=MagicMock(),
        graph=graph,
        env="PROD",
        s3=None,
        report=SqsSourceReport(),
        target_allowed=lambda _name: True,
    )

    found = lineage._datasets(
        BucketNotification(
            bucket="landing", target_arn="arn", prefix=prefix, suffix=suffix
        )
    )

    assert set(found or []) == {_s3_urn(name) for name in matched}


def test_notification_suffix_rule_is_read(aws: None) -> None:
    queue_arn = _queue_arn("uploads")
    _bucket(
        {
            "QueueConfigurations": [
                {
                    "QueueArn": queue_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [
                                {"Name": "prefix", "Value": "incoming/"},
                                {"Name": "suffix", "Value": ".csv"},
                            ]
                        }
                    },
                }
            ]
        }
    )
    client = S3NotificationClient(
        boto3.client("s3", region_name=REGION), SqsSourceReport()
    )

    assert client.get_notifications("landing") == [
        BucketNotification(
            bucket="landing", target_arn=queue_arn, prefix="incoming/", suffix=".csv"
        )
    ]


def test_file_types_match_the_s3_source() -> None:
    assert set(SUPPORTED_FILE_TYPES) == S3_SOURCE_FILE_TYPES
    assert set(SUPPORTED_COMPRESSIONS) == S3_SOURCE_COMPRESSIONS
