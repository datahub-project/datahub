from re import match, compile
from typing import Iterable, Optional

import datahub.emitter.mce_builder as builder
from datahub.configuration import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    DatasetSnapshot
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    SubTypesClass,
    DataPlatformInfoClass,
    DataPlatformSnapshotClass,
)
from google.api_core import page_iterator
from google.cloud import storage
from pydantic.fields import Field, List

NUMERIC_NAMES_PATTERN = compile(r"^[0-9]+")


def recursive_folder_search(
        client: storage.Client,
        folders_set: set,
        bucket_name: str,
        prefix: str = "",
        current_depth: int = 0,
        max_depth: int = 1,
        ignore_numeric_names: bool = False,
        max_node_children=20,
        skip_heavy_nodes=True
):
    def _item_to_value(iterator, item):
        return item

    if current_depth >= max_depth:
        return

    if prefix and not prefix.endswith('/'):
        prefix += '/'

    extra_params = {
        "projection": "noAcl",
        "prefix": prefix,
        "delimiter": '/'
    }

    path = "/b/" + bucket_name + "/o"

    iterator = page_iterator.HTTPIterator(
        client=client,
        api_request=client._connection.api_request,
        path=path,
        items_key='prefixes',
        item_to_value=_item_to_value,
        extra_params=extra_params,
    )

    folders_in_prefix = [folder for folder in iterator]
    if folders_in_prefix:
        if len(folders_in_prefix) >= max_node_children:
            if skip_heavy_nodes:
                return
            else:
                # truncate search
                for folder in folders_in_prefix[:max_node_children]:
                    folder_name = folder.split("/")[-2]
                    if ignore_numeric_names and match(NUMERIC_NAMES_PATTERN, folder_name):
                        continue
                    else:
                        folder_path = f"{bucket_name}/{folder}"
                        if not folder_path in folders_set:
                            folders_set.add(folder_path)
                            recursive_folder_search(
                                client,
                                folders_set,
                                bucket_name,
                                folder,
                                current_depth + 1,
                                max_depth,
                                ignore_numeric_names,
                                max_node_children,
                                skip_heavy_nodes
                            )
        else:
            for folder in folders_in_prefix:
                folder_name = folder.split("/")[-2]
                if ignore_numeric_names and match(NUMERIC_NAMES_PATTERN, folder_name):
                    continue
                else:
                    folder_path = f"{bucket_name}/{folder}"
                    if not folder_path in folders_set:
                        folders_set.add(folder_path)
                        recursive_folder_search(
                            client,
                            folders_set,
                            bucket_name,
                            folder,
                            current_depth + 1,
                            max_depth,
                            ignore_numeric_names,
                            max_node_children,
                            skip_heavy_nodes
                        )
    else:
        return


class GoogleCloudCredential(ConfigModel):
    project_id: str = Field(description="Project id to set the credentials")
    private_key_id: str = Field(description="Private key id")
    private_key: str = Field(
        description="Private key in a form of '-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n'"
    )
    client_email: str = Field(description="Client email")
    client_id: str = Field(description="Client Id")
    auth_uri: str = Field(
        default="https://accounts.google.com/o/oauth2/auth",
        description="Authentication uri",
    )
    token_uri: str = Field(
        default="https://oauth2.googleapis.com/token", description="Token uri"
    )
    auth_provider_x509_cert_url: str = Field(
        default="https://www.googleapis.com/oauth2/v1/certs",
        description="Auth provider x509 certificate url",
    )
    type: str = Field(
        default="service_account", description="Authentication type"
    )
    client_x509_cert_url: Optional[str] = Field(
        default=None,
        description="If not set it will be default to https://www.googleapis.com/robot/v1/metadata/x509/client_email",
    )


class BucketPatternFilter(ConfigModel):
    deny: List[str] = Field(description="Bucket patterns to deny. Ex: 'internal_*'")
    allow: List[str] = Field(description="Bucket patterns to allow. Ex: 'internal_*'")


class FinderConfig(ConfigModel):
    only_alphabet: bool = Field(description="Ignore folders whose name contains only numbers", default=True)
    max_depth: int = Field(description="Max depth of the recursive search", default=5)
    skip_heavy_nodes = Field(description="Max depth of the recursive search", default=True)
    max_node_children = Field(description="Max depth of the recursive search", default=20)

class GoogleStorageSourceConfig(ConfigModel):
    bucket_pattern: BucketPatternFilter = Field(description="Filter patterns for bucket")
    finder_config: FinderConfig = Field(description="Filter patterns for bucket")
    credential: Optional[GoogleCloudCredential] = Field(default=None,
                                                        description="Google Credentials"
                                                        )


@platform_name("Google Storage")
@config_class(GoogleStorageSourceConfig)
@support_status(SupportStatus.UNKNOWN)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class GoogleStorageSource(Source):
    """
    This plugin extracts Charts, dashboards, and associated metadata. This plugin is in beta and has only been tested
    on PostgreSQL and H2 database.
    ### Dashboard

    [/api/dashboard](https://www.metabase.com/docs/latest/api-documentation.html#dashboard) endpoint is used to
    retrieve the following dashboard information.

    - Title and description
    - Last edited by
    - Owner
    - Link to the dashboard in Metabase
    - Associated charts

    ### Chart

    [/api/card](https://www.metabase.com/docs/latest/api-documentation.html#card) endpoint is used to
    retrieve the following information.

    - Title and description
    - Last edited by
    - Owner
    - Link to the chart in Metabase
    - Datasource and lineage

    The following properties for a chart are ingested in DataHub.

    | Name          | Description                                     |
    | ------------- | ----------------------------------------------- |
    | `Dimensions`  | Column names                                    |
    | `Filters`     | Any filters applied to the chart                |
    | `Metrics`     | All columns that are being used for aggregation |


    """

    config: GoogleStorageSourceConfig
    report: SourceReport
    platform: str = "cloud_storage"
    audience: str = "https://www.googleapis.com/oauth2/v4/token"

    def __hash__(self):
        return id(self)

    def __init__(self, config: GoogleStorageSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        self.client = storage.Client.from_service_account_info(self.config.credential.dict())

    def is_permited_bucket(self, bucket_name: str) -> bool:
        deny_patterns = self.config.bucket_pattern.deny
        denied = False
        allow_patterns = self.config.bucket_pattern.allow
        allowed = False
        if deny_patterns:
            for pattern in deny_patterns:
                if match(pattern, bucket_name):
                    denied = True
        if allow_patterns:
            for pattern in allow_patterns:
                if match(pattern, bucket_name):
                    allowed = True
        return allowed and not denied

    def emit_platform_mce(self):
        sorage_platform = DataPlatformInfoClass(
            name=self.platform,
            type="OBJECT_STORE",
            datasetNameDelimiter="/",
            displayName="Cloud Storage",
            logoUrl="https://www.logo.wine/a/logo/Google_Storage/Google_Storage-Logo.wine.svg"
        )
        urn = builder.make_data_platform_urn(self.platform)
        storage_platform_snapshot = DataPlatformSnapshotClass(
            urn=urn,
            aspects=[sorage_platform]
        )
        mce = MetadataChangeEvent(proposedSnapshot=storage_platform_snapshot)
        wu = MetadataWorkUnit(id=urn, mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def construct_folders(self, bucket: storage.bucket.Bucket):
        print(f"searching for folders in {bucket.name}")
        bucket_folders = self.find_folders_from_bucket(bucket)
        print("Pegou todos os folders")
        for folder in bucket_folders:
            if not folder.endswith("/"):
                folder = folder + "/"
            folder_name = f"{folder.split('/')[-2]}_storage".lower()
            folder_urn = builder.make_dataset_urn(self.platform, f"{folder}{folder_name}")

            folder_snapshot = DatasetSnapshot(
                urn=folder_urn,
                aspects=[]
            )
            properties = DatasetPropertiesClass(
                customProperties={},
                name=folder_name,
                externalUrl=f"https://console.cloud.google.com/storage/browser/{folder[:-1]};tab=objects"
            )
            folder_snapshot.aspects.append(properties)
            mce = MetadataChangeEvent(proposedSnapshot=folder_snapshot)
            wu = MetadataWorkUnit(id=folder_urn, mce=mce)
            self.report.report_workunit(wu)
            yield wu
            subtype_wu = MetadataWorkUnit(
                id=f"{folder_urn}-subtype",
                mcp=MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=folder_urn,
                    aspectName="subTypes",
                    aspect=SubTypesClass(typeNames=["storage"]),
                ),
            )
            self.report.report_workunit(subtype_wu)
            yield subtype_wu
            # TODO Create container

    def find_folders_from_bucket(self, bucket: storage.bucket.Bucket) -> List[str]:
        folders = set()
        recursive_folder_search(
            client=self.client,
            folders_set=folders,
            bucket_name=bucket.name,
            prefix="",
            current_depth=0,
            max_depth=self.config.finder_config.max_depth,
            ignore_numeric_names=self.config.finder_config.only_alphabet,
            max_node_children=self.config.finder_config.max_node_children,
            skip_heavy_nodes=self.config.finder_config.skip_heavy_nodes
        )
        return list(folders)

    def emit_folders_mces(self) -> Iterable[MetadataWorkUnit]:
        buckets = list(self.client.list_buckets())
        for bucket in buckets:
            if self.is_permited_bucket(bucket.name):
                yield from self.construct_folders(bucket)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = GoogleStorageSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_platform_mce()
        yield from self.emit_folders_mces()

    def get_report(self) -> SourceReport:
        return self.report
