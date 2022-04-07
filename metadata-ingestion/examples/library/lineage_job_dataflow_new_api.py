import json
import pathlib

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeProposal
from datahub.api.dataflow import DataFlow
from datahub.api.datajob import DataJob
from datahub_provider.entities import Dataset


class FileEmitter:
    def __init__(self, filename: str):
        fpath = pathlib.Path(filename)

        self.file = fpath.open("w")
        self.file.write("[\n")
        self.wrote_something = False

    def emit(self, mcp: MetadataChangeProposal) -> None:
        obj = mcp.to_obj()

        if self.wrote_something:
            self.file.write(",\n")

        json.dump(obj, self.file, indent=4)
        self.wrote_something = True


#emitter = FileEmitter("/Users/treff7es/dataflow-dep.json")


# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

jobFlow = DataFlow(
    cluster="prod", orchestrator="airflow", id="transform_etl_pipeline"
)
jobFlow.emit(emitter)

airflowJob1 = DataJob(flow_urn=jobFlow.urn, id="do_some_random_thing")
airflowJob1.emit(emitter)
airflowJob2 = DataJob(flow_urn=jobFlow.urn, id="runSparkJob")
airflowJob2.input_datajob_urns = [airflowJob1.urn]
airflowJob2.emit(emitter)


jobFlow = DataFlow(
    cluster="PROD", orchestrator="spark", id="spark_app"
)
#jobFlow.emit(emitter)
dataJob3 = DataJob(flow_urn=jobFlow.urn, id="spark_app_union")
#dataJob3.inlets = [Dataset(platform="s3", env="PROD", name="mybucket2/input/2022/02/23"), Dataset(platform="s3", env="PROD", name="mybucket/input/2022/02/23"), Dataset(platform="s3", env="PROD", name="mybucket/input2/2022/02/23")]
#dataJob3.outlets = [Dataset(platform="redshift", env="PROD", name="wd.myschema.db1"), Dataset(platform="redshift", env="PROD", name="wd.myschema.db2")]
dataJob3.input_datajob_urns = [airflowJob2.urn]
dataJob3.emit(emitter)


dataJob2 = DataJob(flow_urn=jobFlow.urn, id="spark_app")
dataJob2.input_datajob_urns = [dataJob3.urn]
dataJob2.inlets = [Dataset(platform="s3", env="PROD", name="mybucket/input/2022/02/23"), Dataset(platform="s3", env="PROD", name="mybucket/input2/2022/02/23")]
dataJob2.outlets = [Dataset(platform="redshift", env="PROD", name="wd.myschema.db1")]
dataJob2.emit(emitter)

dataJob4 = DataJob(flow_urn=jobFlow.urn, id="spark_ap2")
dataJob4.input_datajob_urns = [dataJob3.urn, dataJob2.urn]
dataJob4.inlets = [Dataset(platform="s3", env="PROD", name="mybucket2/input/2022/02/23")]
dataJob4.outlets = [Dataset(platform="redshift", env="PROD", name="wd.myschema.db2")]

dataJob4.emit(emitter)
