from datetime import datetime, timedelta

from airflow import DAG
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DatasetPropertiesClass, DatasetSnapshotClass

from datahub_airflow_plugin.operators.datahub import DatahubEmitterOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "execution_timeout": timedelta(minutes=5),
}


with DAG(
    "datahub_emitter_operator_jinja_template_dag",
    default_args=default_args,
    description="An example dag with jinja template",
    schedule_interval=None,
    tags=["example_tag"],
    catchup=False,
    default_view="tree",
):
    add_custom_properties = DatahubEmitterOperator(
        task_id="datahub_emitter_operator_jinja_template_dag_task",
        mces=[
            MetadataChangeEvent(
                proposedSnapshot=DatasetSnapshotClass(
                    urn="urn:li:dataset:(urn:li:dataPlatform:hive,foursquare.example.lineage_example,DEV)",
                    aspects=[
                        DatasetPropertiesClass(
                            customProperties={"jinjaTemplate": "{{ ds }}"}
                        )
                    ],
                ),
            )
        ],
        datahub_conn_id="datahub_file_default",
    )
