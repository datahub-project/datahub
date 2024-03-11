import pathlib
import tempfile
from typing import Optional
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    StatusClass,
    DataHubConnectionDetailsClass,
    DataHubConnectionDetailsTypeClass,
    DataHubJsonConnectionClass,
    TimeStampClass,
)
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import (
    get_default_graph,
    get_url_and_token,
    DatahubClientConfig,
)
from datahub.ingestion.sink.datahub_rest import (
    DatahubRestSink,
    DatahubRestSinkConfig,
    RecordEnvelope,
)
from datahub.ingestion.api.common import PipelineContext
from datahub_integrations.analytics.s3.connection import S3Connection, S3StaticCreds
from datahub_integrations.graphql.connection import save_connection_json
from fake_form_data import FakeFormData
from datahub.ingestion.source.datahub_reporting.datahub_form_reporting import (
    DataHubFormReportingData,
)
from datahub.configuration.common import ConnectionModel
from datahub.ingestion.api.sink import NoopWriteCallback

# import boto library
import boto3
import sys
import os
import pandas as pd
import json


default_file_path = {
    "local": tempfile.gettempdir() + "/data/forms_verification_data",
    "s3": "s3://acryl-customer-data-f7700a3ff0-usw2/reporting-experiments/form_verification_data",
}


def get_reporting_dataset_urn() -> str:
    form_config_query = """
            query {
                formAnalyticsConfig {
                    datasetUrn
                    physicalUriPrefix
                 }
            }
            """
    with get_default_graph() as graph:
        query_result = graph.execute_graphql(query=form_config_query)
        if query_result:
            dataset_urn = query_result.get("formAnalyticsConfig", {}).get("datasetUrn")
    dataset_urn = dataset_urn or make_dataset_urn("datahub", "local_parquet")
    return dataset_urn


dataset_urn = get_reporting_dataset_urn()


def get_s3_connection() -> S3Connection:
    def get_local_s3_creds() -> S3StaticCreds:
        # open the json file under ~/.aws/cli/cache/
        base_path = os.path.expanduser("~/.aws/cli/cache/")
        # find the latest modified file
        files = os.listdir(base_path)
        files.sort(key=lambda x: os.path.getmtime(base_path + x))
        latest_file = files[-1]
        # open the file
        with open(base_path + latest_file) as f:
            data = json.load(f)
            # return the credentials
            return S3StaticCreds(
                s3_access_key=data["Credentials"]["AccessKeyId"],
                s3_access_secret=data["Credentials"]["SecretAccessKey"],
                s3_session_token=data["Credentials"]["SessionToken"],
            )

    return S3Connection(
        s3_reqion="us-west-2",
        # s3_creds=get_local_s3_creds()
    )


def check_s3_directory_exists(bucket_name, directory_prefix):
    """
    Check if a directory exists in an S3 bucket.

    :param bucket_name: Name of the S3 bucket
    :param directory_prefix: The directory path/prefix in the S3 bucket
    :return: True if the directory exists, False otherwise
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    # Ensure the prefix ends with a '/'
    if not directory_prefix.endswith("/"):
        directory_prefix += "/"

    # Use the paginator to handle buckets with many objects efficiently
    for page in paginator.paginate(
        Bucket=bucket_name, Prefix=directory_prefix, Delimiter="/"
    ):
        # Check for any contents returned by the list operation
        if "Contents" in page:
            return True

    return False


import pyarrow as pa
import pyarrow.parquet as pq


def write_to_parquet(generator_func, parquet_file_path):
    # Define an Arrow schema based on the data structure of the dictionaries
    schema = pa.schema(
        [
            (key, pa.string())  # Adjust the data type as needed
            for key in next(
                generator_func
            )  # Infer the schema from the first dictionary
        ]
    )

    # Open a Parquet writer in append mode
    with pq.ParquetWriter(parquet_file_path, schema) as writer:
        for data_chunk in generator_func:
            # Convert each dictionary chunk to Arrow table
            table = pa.Table.from_pydict(data_chunk)
            # Write the table to the Parquet file
            writer.write_table(table)
    return "file://" + file_path


def create_s3_parquet_file(force_create, s3_uri, df):
    # first check if the file exists, if not skip
    # use boto3 to check if the file exists
    # if --force-create is set in the command line, then create the file
    bucket, key = s3_uri.replace("s3://", "").split("/", 1)
    if force_create:
        pass
    else:
        s3 = boto3.client("s3")
        try:
            # first split the s3_uri to get the bucket and key
            s3.head_object(Bucket=bucket, Key=key)
            print("File already exists")
            return s3_uri
        except:
            pass

    # create a temp dir and a file in the temp dir
    temp_path = tempfile.mkdtemp()
    file_path = f"{temp_path}/{bucket}/{key}"
    # mkdir -p
    # os.mkdir(file_path, exist_ok=True)
    pathlib.Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    # write to a local parquet file
    create_local_parquet_file(True, file_path, df)
    s3 = boto3.client("s3")
    s3.upload_file(file_path, bucket, key)
    print(f"Created file {default_file_path['s3']}")
    os.remove(file_path)
    return s3_uri


def create_local_parquet_file(force_create, file_path, df):
    # First check if the file exists, if not, create it
    if not pathlib.Path(file_path).exists() or force_create:
        # first create the directory
        directory = pathlib.Path(file_path).parent
        directory.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_path, compression="snappy")
        print(f"Created file {file_path}")
    else:
        print(f"File {file_path} already exists")
    return "file://" + file_path


def register_connection(connection_id: str, connection_model: ConnectionModel) -> str:
    connection_urn = f"urn:li:dataHubConnection:{connection_id}"
    try:
        save_connection_json(
            graph=get_default_graph(),
            urn=connection_urn,
            platform_urn="urn:li:dataPlatform:datahub",
            config=connection_model,
            name="analytics connection",
        )
        print(f"Connection {connection_urn} created successfully")
        return connection_urn
    except Exception as e:
        print(f"Error creating connection {connection_urn}: {e}")
        print(f"Continuing without creating connection {connection_urn}")
        return None


def register_dataset(dataset_uri: str, connection_urn: Optional[str]):
    # Now register the dataset in DataHub
    import time

    dataset_properties = DatasetPropertiesClass(
        name="local_parquet",
        description="A dataset for local testing",
        externalUrl=dataset_uri,
        uri=None,
        customProperties=(
            {
                "connection_urn": connection_urn,
            }
            if connection_urn
            else None
        ),
        lastModified=TimeStampClass(time=int(time.time() * 1000)),
    )

    mcps = MetadataChangeProposalWrapper.construct_many(
        entityUrn=dataset_urn,
        aspects=[
            dataset_properties,
            StatusClass(
                removed=True  # Registering this is as a soft deleted asset to ensure it doesn't show up in search
            ),
        ],
    )

    with get_default_graph() as graph:
        for mcp in mcps:
            graph.emit_mcp(mcp)

    print(f"Dataset {dataset_urn} created successfully")


from datetime import datetime, timedelta


class DateRange:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.current_date = start_date

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_date > self.end_date:
            raise StopIteration
        else:
            today = self.current_date
            self.current_date += timedelta(days=1)
            return today


import click


def get_file_path(platform, date):
    platform_base_path = default_file_path[platform]
    return f"{platform_base_path}/{date.strftime('%Y-%m-%d')}/data.parquet"


def parse_date(ctx, param, value):
    """Callback function to parse date string into a datetime.date object."""
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise click.BadParameter("Date must be in the format YYYY-MM-DD.")


@click.command()
@click.option("--force-create", is_flag=True, help="Force create the parquet file")
@click.option(
    "--file-platform",
    type=str,
    help="The platform to use for the file",
    default="local",
)
@click.option(
    "--from-date", type=str, help="The start date for the dataset", callback=parse_date
)
@click.option(
    "--to-date", type=str, help="The end date for the dataset", callback=parse_date
)
@click.option(
    "--num-assigned-assets", type=int, help="The number of assigned assets", default=100
)
@click.option("--to-datahub-mcps", is_flag=True, help="Generate fake data into DataHub")
@click.option(
    "--to-file",
    is_flag=True,
    help="When set, will write mcps to a file instead of emitting them over the wire",
)
@click.option(
    "--from-datahub", is_flag=True, help="Generate reporting data from DataHub"
)
def main(
    force_create,
    file_platform,
    from_date,
    to_date,
    num_assigned_assets,
    to_datahub_mcps,
    from_datahub,
    to_file,
):
    dataset_uri = None
    connection_urn = None
    if not from_date:
        from_date = (datetime.now() - timedelta(days=1)).date()
    if not to_date:
        to_date = datetime.now().date()
    for today_date in DateRange(from_date, to_date):
        if from_datahub:
            form_data = DataHubFormReportingData(get_default_graph())
        else:
            form_data = FakeFormData(
                num_assigned_assets=num_assigned_assets,
                today_date=today_date,
                previous_form_data=None,
            )

        if to_datahub_mcps:
            # Generate fake data into DataHub
            from progressbar import progressbar

            pipeline_context = PipelineContext(run_id="local")
            from datahub.ingestion.sink.file import FileSink, FileSinkConfig

            if to_file:
                file_directory = "mcp_files"
                os.makedirs(file_directory, exist_ok=True)
                sink = FileSink(
                    ctx=pipeline_context,
                    config=FileSinkConfig(
                        filename=f"{file_directory}/fake_forms_data_0.json"
                    ),
                )
            else:
                (url, token) = get_url_and_token()
                rest_sink_config = DatahubRestSinkConfig(
                    server=url, token=token, max_threads=40
                )
                sink = DatahubRestSink(ctx=pipeline_context, config=rest_sink_config)

            total = 0
            batch_index = 0
            for mcp in progressbar(form_data.to_mcps()):
                total += 1
                sink.write_record_async(
                    RecordEnvelope(record=mcp, metadata={}),
                    write_callback=NoopWriteCallback(),
                )
                if total % 100000 == 0:
                    batch_index += 1
                    if to_file:
                        sink.close()
                        sink = FileSink(
                            ctx=pipeline_context,
                            config=FileSinkConfig(
                                filename=f"{file_directory}/fake_forms_data_{batch_index}.json"
                            ),
                        )
            sink.close()
            print("Done emitting MCPS")
        else:
            from datahub.ingestion.source.datahub_reporting.datahub_dataset import (
                DataHubBasedS3Dataset,
                DatasetMetadata,
                DatasetRegistrationSpec,
                FileStoreBackedDatasetConfig,
                SchemaField,
            )

            registration_spec = DatasetRegistrationSpec()
            dataset_metadata = DatasetMetadata(
                displayName="Fake Forms Reporting Data",
                description=f"This data was generated by the fake forms generator on {today_date}",
                schemaFields=[
                    SchemaField(name=k, type=v)
                    for k, v in {
                        "form_urn": "string",
                        "form_type": "string",
                        "form_created_date": "date32[day]",
                        "form_assigned_date": "date32[day]",
                        "form_completed_date": "date32[day]",
                        "form_status": "string",
                        "question_id": "string",
                        "question_status": "string",
                        "question_completed_date": "date32[day]",
                        "assignee_urn": "string",
                        "asset_urn": "string",
                        "platform_urn": "string",
                        "platform_instance_urn": "string",
                        "domain_urn": "string",
                        "parent_domain_urn": "string",
                        "snapshot_date": "date32[day]",
                        "asset_verified": "bool",
                    }.items()
                ],
            )
            dataset_config = FileStoreBackedDatasetConfig(
                dataset_name="fake name",
                store_platform=file_platform,
                dataset_registration_spec=registration_spec,
                bucket_prefix="s3://reporting-experiments/fake-data-test",
            )
            dataset = DataHubBasedS3Dataset(
                dataset_metadata=dataset_metadata,
                config=dataset_config,
            )
            for row in form_data.get_data():
                dataset.append(row)
            # dataset.write_df(form_data.get_dataframe())
            with get_default_graph() as graph:
                for mcp in dataset.commit():
                    graph.emit_mcp(mcp)
                    dataset_urn = mcp.entityUrn

            print(f"Dataset {dataset_urn} created successfully")

            # # # Generate fake reporting data into a parquet file
            # # file_path = get_file_path(platform=file_platform, date=today_date)
            # # if file_platform == "local":
            # #     dataset_uri = write_to_parquet(form_data.get_data, file_path)
            # #     # dataset_uri = create_local_parquet_file(
            # #     #     force_create, file_path, form_data.get_dataframe()
            # #     # )
            # # elif file_platform == "s3":
            # #     dataset_uri = create_s3_parquet_file(
            # #         force_create, file_path, form_data.get_dataframe()
            # #     )
            # #     print(f"Dataset URI: {dataset_uri}")
            # #     s3_connection = get_s3_connection()
            # #     connection_urn = register_connection("analytics_s3", s3_connection)
            # # else:
            # #     print("Invalid platform")
            # if dataset_uri:
            #     register_dataset(dataset_uri, connection_urn)


if __name__ == "__main__":
    main()
