import datetime
import logging
import os
import pathlib
import tempfile
import time
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import boto3
import duckdb
import pandas
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, validator

from acryl_datahub_cloud.elasticsearch.graph_service import BaseModelRow, SchemaField
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    DatasetFieldProfileClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    NumberTypeClass,
    OperationClass,
    OperationTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    StringTypeClass,
    TimeStampClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)


class PartitioningStrategy(str, Enum):
    DATE = "date"
    SNAPSHOT = "snapshot"


class DatasetRegistrationSpec(BaseModel):
    """
    Specifications for dataset to register
    """

    soft_deleted: bool = (
        True  # When set to True, the dataset will be registered as a soft-deleted asset
    )
    profile: bool = (
        True  # When set to True, the dataset will be profiled before registering
    )
    last_modified: bool = (
        True  # When set to True, the last modified timestamp will be updated
    )
    operation_aspect: bool = (
        True  # When set to True, the operation aspect will be added to the MCE
    )


class FileStoreBackedDatasetConfig(ConfigModel):
    dataset_name: str
    dataset_urn: Optional[str] = (
        None  # If not set, it will be generated from the dataset_name
    )
    bucket_prefix: str
    store_platform: str = "s3"
    file_name: str = "data"
    file_extension: str = "parquet"
    file_compression: str = "snappy"
    file_overwrite_existing: bool = True
    snapshot_partitioning_strategy: str = PartitioningStrategy.DATE
    generate_presigned_url: bool = True
    presigned_url_expiry_days: int = 7
    dataset_registration_spec: DatasetRegistrationSpec = DatasetRegistrationSpec()
    file: Optional[str] = (
        None  # This is the file to be registered. When set, the file will be registered as a dataset immediately
    )

    datahub_platform: str = "acryl"

    @validator("snapshot_partitioning_strategy")
    def validate_partitioning_strategy(cls, v):
        if v not in PartitioningStrategy._value2member_map_:
            raise ValueError(f"Unsupported partitioning strategy: {v}")
        return v

    @staticmethod
    def dummy():
        return FileStoreBackedDatasetConfig(dataset_name="none", bucket_prefix="none")


class DatasetMetadata(BaseModel):
    schemaFields: Optional[List[SchemaField]] = None
    displayName: Optional[str] = None
    description: Optional[str] = None


class DataHubBasedS3Dataset:
    def __init__(
        self,
        config: FileStoreBackedDatasetConfig,
        dataset_metadata: DatasetMetadata,
    ):
        self.s3_client = boto3.client("s3")
        self.config = config
        self.dataset_metadata = dataset_metadata
        self.opened_files: List[str] = []
        self.first_row = True
        self.current_record_batch: Optional[
            List[Union[Dict[str, Any], BaseModelRow]]
        ] = None
        self.local_file_path: str = (
            config.file if config.file else self._initialize_local_file()
        )
        self.file_writer = None
        self.schema = (
            pa.schema([(x.name, x.type) for x in self.dataset_metadata.schemaFields])
            if self.dataset_metadata.schemaFields
            else None
        )

    def get_dataset_urn(self) -> str:
        return self.config.dataset_urn or make_dataset_urn(
            self.config.datahub_platform, self.config.dataset_name
        )

    def get_file_uri(self) -> str:
        return self.get_remote_file_uri(
            self.config.bucket_prefix,
            date=(
                datetime.date.today()
                if self.config.snapshot_partitioning_strategy
                == PartitioningStrategy.DATE
                else None
            ),
        )

    def _initialize_local_file(self) -> str:
        # create a temp dir and a file in the temp dir
        temp_path = tempfile.mkdtemp()
        bucket, key = self.get_file_uri().replace("s3://", "").split("/", 1)
        file_path = f"{temp_path}/{bucket}/{key}"
        pathlib.Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        return file_path

    def append(self, row: Union[Dict[str, Any], BaseModelRow]) -> None:
        if self.first_row:
            self.first_row = False
            self.current_record_batch = []
            self.stringify_row: bool = False

            if not self.schema:
                if isinstance(row, BaseModelRow):
                    # BaseModelRow allows us to introspect the schema and
                    # generate an arrow schema from it
                    self.schema = row.arrow_schema()
                else:
                    # hail mary: infer schema from the first row and cast everything to string
                    self.schema = pa.schema([(key, pa.string()) for key in row.keys()])
                    self.stringify_row = True

            self._initialize_local_file()
            self.file_writer = pq.ParquetWriter(
                self.local_file_path,
                self.schema,
                compression=self.config.file_compression,
            )
        if isinstance(row, BaseModel) or isinstance(row, BaseModelRow):
            # for anything extending BaseModel, we want to use the dict representation
            write_row: Dict[str, Any] = row.dict()
        elif isinstance(row, dict):
            write_row = row
        else:
            raise ValueError(
                f"Unsupported type for row: {type(row)}. Must be a dict or a BaseModel"
            )
        # At this point we know that write_row is a dict
        assert isinstance(write_row, dict)
        if self.stringify_row:
            write_row = {k: str(v) for k, v in write_row.items()}
        assert self.current_record_batch is not None
        self.current_record_batch.append(write_row)
        if len(self.current_record_batch) > 1024 * 1024:
            self._write_current_batch()

    def _write_current_batch(self):
        assert self.file_writer is not None
        assert self.schema is not None
        self.file_writer.write_batch(
            pa.RecordBatch.from_pandas(
                pandas.DataFrame(self.current_record_batch), schema=self.schema
            )
        )
        self.current_record_batch = []

    def commit(self) -> Iterable[MetadataChangeProposalWrapper]:
        if self.file_writer is not None and self.file_writer.is_open:
            self._write_current_batch()
            self.file_writer.close()
        if self.local_file_path:
            self.opened_files.append(self.local_file_path)
        if not os.path.exists(self.local_file_path):
            logger.info(f"File not found: {self.local_file_path}")
            return
        if os.path.getsize(self.local_file_path) == 0:
            logger.info(f"File is empty: {self.local_file_path}")
            return
        if self.config.store_platform == "s3":
            self.upload_file_to_s3()
        yield from self._register_dataset()

    def upload_file_to_s3(self):
        bucket, key = self.get_file_uri().replace("s3://", "").split("/", 1)
        assert self.s3_client is not None
        assert self.local_file_path is not None
        logger.info(f"Uploading {self.local_file_path} to s3://{bucket}/{key}")
        self.s3_client.upload_file(self.local_file_path, bucket, key)

    def _register_dataset(self) -> Iterable[MetadataChangeProposalWrapper]:
        dataset_urn = self.get_dataset_urn()
        assert self.local_file_path is not None
        yield from self.register_dataset(
            dataset_urn, self.get_file_uri(), self.local_file_path
        )

    def _generate_presigned_url(self, s3_uri: str) -> str:
        """
        Generate a pre-signed URL for downloading an object from S3.

        Args:
        - s3_uri (str): The S3 URI of the object to be downloaded (e.g., 's3://bucket-name/object-key').
        - expiration (int): Expiration time for the pre-signed URL in seconds (default is 3600 seconds).

        Returns:
        - str: The pre-signed URL for downloading the object.
        """
        # Parse the S3 URI to get bucket name and object key
        bucket_name, object_key = s3_uri.replace("s3://", "").split("/", 1)
        expiration_seconds = self.config.presigned_url_expiry_days * 24 * 60 * 60
        # Generate a pre-signed URL for downloading the object
        presigned_url = self.s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_key},
            ExpiresIn=expiration_seconds,
        )

        return presigned_url

    def get_remote_file_uri(
        self, dataset_uri_prefix: str, date: Optional[datetime.date] = None
    ) -> str:
        if self.config.snapshot_partitioning_strategy == PartitioningStrategy.DATE:
            assert date is not None
            assert dataset_uri_prefix is not None
            # TODO: This should be date.strftime('year=%Y/month=%m/day=%d') for correct time partition in S3
            return f"{dataset_uri_prefix.rstrip('/')}/{date.strftime('%Y-%m-%d')}/{self.config.file_name}.{self.config.file_extension}"
        elif (
            self.config.snapshot_partitioning_strategy == PartitioningStrategy.SNAPSHOT
        ):
            return f"{dataset_uri_prefix.rstrip('/')}/{self.config.file_name}.{self.config.file_extension}"
        else:
            raise ValueError(
                f"Unsupported partitioning strategy: {self.config.snapshot_partitioning_strategy}"
            )

    def _generate_schema_metadata(
        self, duckdb_columns: List[Tuple[str, str]]
    ) -> SchemaMetadataClass:
        def get_type_from_dtype(dtype: str) -> SchemaFieldDataTypeClass:
            if "int" in dtype:
                return SchemaFieldDataTypeClass(type=NumberTypeClass())
            elif "float" in dtype:
                return SchemaFieldDataTypeClass(type=NumberTypeClass())
            elif "number" in dtype:
                return SchemaFieldDataTypeClass(type=NumberTypeClass())
            elif "bool" in dtype:
                return SchemaFieldDataTypeClass(type=BooleanTypeClass())
            elif "datetime" in dtype or "date" in dtype:
                return SchemaFieldDataTypeClass(type=TimeTypeClass())
            else:
                return SchemaFieldDataTypeClass(type=StringTypeClass())

        # for each column -> generate min, max, avg, distinct count, null count
        # for the dataset -> generate row count, number of columns
        # for each column -> generate data type
        audit_stamp = AuditStampClass(
            time=int(time.time()) * 1000, actor="urn:li:corpuser:datahub"
        )
        schema_metadata = SchemaMetadataClass(
            created=audit_stamp,
            lastModified=audit_stamp,
            hash="",
            platform="urn:li:dataPlatform:s3",
            version=0,
            primaryKeys=[],
            schemaName="",
            fields=[],
            platformSchema=OtherSchemaClass(rawSchema=str(self.schema)),
        )
        for column in duckdb_columns:
            # generate data type
            data_type = column[1].lower()
            schema_metadata.fields.append(
                SchemaFieldClass(
                    fieldPath=column[0],
                    type=get_type_from_dtype(data_type),
                    nativeDataType=str(data_type),
                )
            )
        return schema_metadata

    def _generate_dataset_profile_and_schema(
        self, file: str
    ) -> Tuple[DatasetProfileClass, SchemaMetadataClass]:
        # for each column -> generate min, max, avg, distinct count, null count
        # for the dataset -> generate row count, number of columns
        # for each column -> generate data type

        from_fragment = f"FROM parquet_scan('{file}')"
        with duckdb.connect() as conn:
            # duckdb.execute(
            #     f"CREATE TABLE forms AS SELECT * FROM parquet_scan('{file}')"
            # )
            row_count = conn.execute("SELECT COUNT(*) " + from_fragment).fetchall()[0][
                0
            ]
            columns = conn.execute("SELECT * " + from_fragment + " LIMIT 1").description
            logger.info("Generating dataset profile")
            assert columns is not None
            dataset_profiles = DatasetProfileClass(
                timestampMillis=int(time.time()) * 1000,
                rowCount=row_count,
                columnCount=len(columns),
                fieldProfiles=[],
            )
            assert isinstance(columns, list)
            for column in columns:
                # generate min, max, avg, distinct count, null count
                column_name = column[0]
                logger.info(f"Generating field profile for {column_name}")
                data_type = column[1].lower()
                if "int" in data_type or "float" in data_type:
                    query = (
                        f"SELECT COUNT(DISTINCT {column_name}), COUNT(*) - COUNT({column_name}), MIN({column_name}), MAX({column_name}), AVG({column_name})"
                        + from_fragment
                    )
                    (
                        distinct_count,
                        null_count,
                        min_value,
                        max_value,
                        avg_value,
                    ) = conn.execute(query).fetchall()[0]
                elif "date" in data_type:
                    query = (
                        f"SELECT COUNT(DISTINCT {column_name}), COUNT(*) - COUNT({column_name}), MIN({column_name}), MAX({column_name})"
                        + from_fragment
                    )
                    (distinct_count, null_count, min_value, max_value) = conn.execute(
                        query
                    ).fetchall()[0]
                    avg_value = None
                else:
                    query = (
                        f"SELECT COUNT(DISTINCT {column_name}), COUNT(*) - COUNT({column_name})"
                        + from_fragment
                    )
                    (distinct_count, null_count) = conn.execute(query).fetchall()[0]
                    min_value = None
                    max_value = None
                    avg_value = None
                # get 10 sample values
                sample_values = [
                    str(x[0])
                    for x in conn.execute(
                        f"SELECT {column_name} " + from_fragment + " using SAMPLE 10"
                    ).fetchall()
                ]
                print(
                    f"Column: {column}, Min: {min_value}, Max: {max_value}, Avg: {avg_value}, Distinct: {distinct_count}, Null: {null_count}, Data Type: {data_type}"
                )
                field_profile = DatasetFieldProfileClass(
                    fieldPath=column[0],
                    min=str(min_value),
                    max=str(max_value),
                    mean=str(avg_value),
                    uniqueCount=distinct_count,
                    uniqueProportion=float(distinct_count) * 100.0 / float(row_count),
                    nullCount=null_count,
                    nullProportion=float(null_count) * 100.0 / float(row_count),
                    sampleValues=sample_values,
                )
                assert dataset_profiles.fieldProfiles is not None
                dataset_profiles.fieldProfiles.append(field_profile)
            logger.info("Generated dataset profile")
            schema_metadata = self._generate_schema_metadata(columns)
        return dataset_profiles, schema_metadata

    def register_dataset(
        self,
        dataset_urn: str,
        physical_uri: str,
        local_file: str,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        aspects: List = []
        mcps: List[MetadataChangeProposalWrapper] = self._update_presigned_url(
            dataset_urn, physical_uri
        )
        current_time_millis = int(time.time()) * 1000
        aspects.extend(
            [
                StatusClass(
                    removed=self.config.dataset_registration_spec.soft_deleted  # Registering this is as a visible asset as we have search access policies to ensure only admins can see it
                ),
                OperationClass(
                    timestampMillis=current_time_millis,
                    operationType=OperationTypeClass.UPDATE,
                    lastUpdatedTimestamp=current_time_millis,
                ),
            ]
        )

        if self.config.dataset_registration_spec.profile:
            (
                dataset_profiles,
                schema_metadata,
            ) = self._generate_dataset_profile_and_schema(local_file or physical_uri)
            aspects.append(dataset_profiles)
            aspects.append(schema_metadata)
        mcps.extend(
            MetadataChangeProposalWrapper.construct_many(
                entityUrn=dataset_urn,
                aspects=aspects,
            )
        )
        yield from mcps

    def update_presigned_url(
        self, dataset_properties: Optional[DatasetPropertiesClass]
    ) -> List[MetadataChangeProposalWrapper]:
        return self._update_presigned_url(
            self.get_dataset_urn(), self.get_file_uri(), dataset_properties
        )

    def _update_presigned_url(
        self,
        dataset_urn: str,
        physical_uri: str,
        dataset_properties: Optional[DatasetPropertiesClass] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        if self.config.generate_presigned_url:
            external_url = self._generate_presigned_url(physical_uri)
        else:
            external_url = physical_uri

        logger.info(
            f"Registering dataset {dataset_urn} with physical URI {physical_uri}"
        )
        if dataset_properties is not None:
            dataset_properties.externalUrl = external_url
        else:
            current_time_millis = int(time.time()) * 1000
            dataset_properties = DatasetPropertiesClass(
                name=self.dataset_metadata.displayName or self.config.dataset_name,
                description=self.dataset_metadata.description or "",
                externalUrl=external_url,
                uri=None,  # Unfortunately, there is a bug in URI validation that requires this to be None
                lastModified=TimeStampClass(
                    time=current_time_millis, actor="urn:li:corpuser:datahub"
                ),
                customProperties={
                    "physical_uri": physical_uri,
                },
            )

        return MetadataChangeProposalWrapper.construct_many(
            entityUrn=dataset_urn,
            aspects=[dataset_properties],
        )
