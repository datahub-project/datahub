from dataclasses import dataclass, field
from typing import Iterable, List, Optional

import pymongo

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DatasetPropertiesClass

# These are MongoDB-internal databases, which we want to skip.
# See https://docs.mongodb.com/manual/reference/local-database/ and
# https://docs.mongodb.com/manual/reference/config-database/ and
# https://stackoverflow.com/a/48273736/5004662.
DENY_DATABASE_LIST = set(["admin", "config", "local"])


class MongoDBConfig(ConfigModel):
    # See the MongoDB authentication docs for details and examples.
    # https://pymongo.readthedocs.io/en/stable/examples/authentication.html
    connect_uri: str = "mongodb://localhost"
    username: Optional[str] = None
    password: Optional[str] = None
    authMechanism: Optional[str] = None
    options: dict = {}

    database_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    collection_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclass
class MongoDBSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


@dataclass
class MongoDBSource(Source):
    config: MongoDBConfig
    report: MongoDBSourceReport

    def __init__(self, ctx: PipelineContext, config: MongoDBConfig):
        super().__init__(ctx)
        self.config = config
        self.report = MongoDBSourceReport()

        options = {}
        if self.config.username is not None:
            options["username"] = self.config.username
        if self.config.password is not None:
            options["password"] = self.config.password
        if self.config.authMechanism is not None:
            options["authMechanism"] = self.config.authMechanism
        options = {
            **options,
            **self.config.options,
        }

        self.mongo_client = pymongo.MongoClient(self.config.connect_uri, **options)

        # This cheaply tests the connection. For details, see
        # https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient
        self.mongo_client.admin.command("ismaster")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = MongoDBConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        env = "PROD"
        platform = "mongodb"

        database_names: List[str] = self.mongo_client.list_database_names()
        for database_name in database_names:
            if database_name in DENY_DATABASE_LIST:
                continue
            if not self.config.database_pattern.allowed(database_name):
                self.report.report_dropped(database_name)
                continue

            database = self.mongo_client[database_name]
            collection_names: List[str] = database.list_collection_names()
            for collection_name in collection_names:
                dataset_name = f"{database_name}.{collection_name}"
                if not self.config.collection_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                mce = MetadataChangeEvent()
                dataset_snapshot = DatasetSnapshot()
                dataset_snapshot.urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})"

                dataset_properties = DatasetPropertiesClass(
                    tags=[],
                    customProperties={},
                )
                dataset_snapshot.aspects.append(dataset_properties)

                # TODO: Guess the schema via sampling
                # State of the art seems to be https://github.com/variety/variety.

                # TODO: use list_indexes() or index_information() to get index information
                # See https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.list_indexes.

                mce.proposedSnapshot = dataset_snapshot

                wu = MetadataWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

    def get_report(self) -> MongoDBSourceReport:
        return self.report

    def close(self):
        self.mongo_client.close()
