"""Shared pieces of the S3 and GCS probe tests.

Both probes talk to storage through boto3's S3 client (GCS through its
S3-compatible endpoint), so the same error builder, operation recorder and
probe-versus-ingestion parity harness serve both. An `ObjectStorePlatform`
holds what differs per connector, and its methods run the parity checks.
"""

import contextlib
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterator,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Set,
    Tuple,
    Type,
)
from unittest import mock

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec
from datahub.ingestion.source.data_lake_common.path_spec_verdict import (
    TEMPLATED_FILE_RULES_WARNING,
    UNPARSED_TABLE_WARNING,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.utilities.urns.dataset_urn import DatasetUrn


def client_error(
    code: str = "AccessDenied",
    status: int = 403,
    operation: str = "ListObjectsV2",
    message: str = "",
) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {
                "RequestId": "",
                "HostId": "",
                "HTTPStatusCode": status,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
            },
        },
        operation,
    )


@contextlib.contextmanager
def recording_s3_operations() -> Iterator[List[str]]:
    """Yields the names of the S3 API operations every boto3 client created
    inside the block issues, in order."""
    seen: List[str] = []
    real_client = boto3.session.Session.client

    def recording_client(self: boto3.session.Session, *args: Any, **kwargs: Any) -> Any:
        client = real_client(self, *args, **kwargs)
        client.meta.events.register(
            "before-call.s3", lambda model, **_: seen.append(model.name)
        )
        return client

    with mock.patch.object(boto3.session.Session, "client", recording_client):
        yield seen


# The parity fixture: one tree, laid out so that each case below exercises a
# path-spec rule, the declared {table}-folder gap (data/docs holds no CSV), and
# the naming gap (a bucket wildcard ingestion cannot parse).
PARITY_OBJECTS: Sequence[Tuple[str, str, str]] = [  # bucket, key, content type
    ("my-bucket", "data/events/year=2024/part-0.csv", "text/csv"),
    ("my-bucket", "data/users/year=2024/part-0.csv", "text/csv"),
    ("my-bucket", "data/_staging/year=2024/part-0.csv", "text/csv"),
    ("my-bucket", "data/docs/year=2024/readme.txt", "text/plain"),
    ("my-bucket", "raw/a.csv", "text/csv"),
    ("my-bucket", "raw/tmp_b.csv", "text/csv"),
    ("my-bucket", "raw/notes.txt", "text/plain"),
    ("my-bucket", "no_ext/blob", "text/csv"),
    ("my-bucket-2", "data/orders/year=2024/part-0.csv", "text/csv"),
]
_CSV = b"a,b\n1,2\n"


class _ObjectStoreConfig(Protocol):
    @property
    def path_specs(self) -> List[PathSpec]: ...

    @property
    def env(self) -> str: ...

    @property
    def platform_instance(self) -> Optional[str]: ...

    @property
    def convert_urns_to_lowercase(self) -> bool: ...


@dataclass(frozen=True)
class ObjectStorePlatform:
    """What the parity harness needs to know about one object-store connector."""

    source_type: str
    scheme: str
    source_class: Type[Source]
    parse_config: Callable[[Dict[str, object]], _ObjectStoreConfig]
    credentials: Mapping[str, object]
    # The PathSpecs ingestion matches s3:// keys against, and whether it
    # ignores file extensions (S3's use_s3_content_type).
    file_rules: Callable[[Dict[str, object]], Tuple[List[PathSpec], bool]]
    # Whether the fixture's objects carry a content type, which only S3 reads.
    sets_content_type: bool
    # Patches that point the connector at moto.
    patches: Sequence[Tuple[str, object]] = ()
    # Cases whose fixture ingestion emits nothing; every other must emit, or it
    # checks nothing.
    empty_cases: FrozenSet[str] = field(default_factory=frozenset)

    def recipe(self, case: Mapping[str, object]) -> Dict[str, object]:
        return {**case, **self.credentials}

    def urn(self, recipe: Dict[str, object], uri: str) -> str:
        config = self.parse_config(recipe)
        path = self._path(uri)
        if config.convert_urns_to_lowercase:
            path = path.lower()
        return make_dataset_urn_with_platform_instance(
            self.source_type, path, config.platform_instance, config.env
        )

    def _path(self, uri: str) -> str:
        prefix = f"{self.scheme}://"
        assert uri.startswith(prefix), uri
        return uri[len(prefix) :].strip("/")

    @contextlib.contextmanager
    def storage(self) -> Iterator[None]:
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock_aws())
            for target, value in self.patches:
                stack.enter_context(mock.patch(target, value))
            client = boto3.client("s3", region_name="us-east-1")
            for bucket in sorted({b for b, _, _ in PARITY_OBJECTS}):
                client.create_bucket(Bucket=bucket)
            for bucket, key, content_type in PARITY_OBJECTS:
                if self.sets_content_type:
                    client.put_object(
                        Bucket=bucket, Key=key, Body=_CSV, ContentType=content_type
                    )
                else:
                    client.put_object(Bucket=bucket, Key=key, Body=_CSV)
            yield

    def ingested(self, recipe: Dict[str, object]) -> Set[str]:
        source = self.source_class.create(
            recipe, PipelineContext(run_id=f"{self.source_type}-probe-parity")
        )
        try:
            return {
                str(wu.metadata.entityUrn)
                for wu in source.get_workunits()
                if isinstance(wu.metadata, MetadataChangeProposalWrapper)
                and isinstance(wu.metadata.aspect, DatasetPropertiesClass)
            }
        finally:
            source.close()

    def probed(self, recipe: Dict[str, object]) -> Tuple[Dict[str, str], List[str]]:
        """urn -> candidate uri for every name probe filter includes, plus its
        warnings."""
        config = self.parse_config(recipe)
        names: Set[str] = set()
        for i, spec in enumerate(config.path_specs):
            if spec.emit_folders_only:
                continue
            listing = run_probe_method(
                self.source_type,
                recipe,
                "datasets",
                {"path_spec": str(i), "limit": "1000"},
            )
            assert not listing.truncated and not listing.failures
            assert isinstance(listing.result, list)
            names |= {d["name"] for d in listing.result}
        verdicts = check_filters(
            source_type=self.source_type,
            config_dict=recipe,
            kind="Table",
            parent_path=[],
            names=sorted(names),
        )
        included = {
            self.urn(recipe, r.name): r.name for r in verdicts.results if r.included
        }
        return included, verdicts.warnings

    def no_allowed_file_under(self, recipe: Dict[str, object], folder_uri: str) -> bool:
        """The one gap the probe declares: a {table} folder whose files all fail
        the spec's file rules. Listing it proves that is why ingestion skipped
        it."""
        specs, ignore_ext = self.file_rules(recipe)
        bucket, _, prefix = self._path(folder_uri).partition("/")
        client = boto3.client("s3", region_name="us-east-1")
        keys = [
            o["Key"]
            for o in client.list_objects_v2(Bucket=bucket, Prefix=prefix + "/").get(
                "Contents", []
            )
        ]
        # No keys: the candidate is an object, not a table folder, and an extra
        # object is never the declared gap.
        return bool(keys) and not any(
            spec.allowed(f"s3://{bucket}/{key}", ignore_ext=ignore_ext)
            for spec in specs
            for key in keys
        )

    def named_after_a_file_in(
        self, recipe: Dict[str, object], ingested: Set[str], probed: Dict[str, str]
    ) -> Dict[str, str]:
        """ingested urn -> the included table folder it was emitted for, for the
        datasets ingestion named after a file rather than the folder. Each must
        sit under an included folder, and judging that folder alone must warn."""
        attributed: Dict[str, str] = {}
        for urn in ingested - set(probed):
            path = DatasetUrn.from_string(urn).name
            folders = [
                u for u in probed.values() if path.startswith(self._path(u) + "/")
            ]
            assert len(folders) == 1, f"probe missed {urn}"
            alone = check_filters(
                source_type=self.source_type,
                config_dict=recipe,
                kind="Table",
                parent_path=[],
                names=folders,
            )
            assert UNPARSED_TABLE_WARNING in alone.warnings, (
                f"{urn} was named without a warning"
            )
            attributed[urn] = folders[0]
        return attributed

    def assert_verdicts_match_ingestion(
        self, name: str, recipe: Dict[str, object]
    ) -> None:
        """The probe never under-reports, and over-reports only where it says it
        cannot judge, or for a folder ingestion named after a file inside it."""
        ingested = self.ingested(recipe)
        assert bool(ingested) is (name not in self.empty_cases)
        if not ingested:
            # Then the probe must list the candidate and judge it excluded, not
            # agree by listing nothing.
            listing = run_probe_method(self.source_type, recipe, "datasets", {})
            assert isinstance(listing.result, list) and listing.result
        probed, warnings = self.probed(recipe)
        renamed = set(self.named_after_a_file_in(recipe, ingested, probed).values())
        for urn in set(probed) - ingested:
            if probed[urn] in renamed:
                continue
            assert self.no_allowed_file_under(recipe, probed[urn]), (
                f"probe included {probed[urn]}, ingestion did not, and a file "
                f"there passes the spec: a verdict rule disagrees with ingestion"
            )
            assert TEMPLATED_FILE_RULES_WARNING in warnings
