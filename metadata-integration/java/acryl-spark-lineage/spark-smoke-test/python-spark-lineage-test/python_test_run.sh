#export SPARK_HOME=$1
#export PATH=$PATH:$1/bin
#export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
#export PATH=$SPARK_HOME/python:$PATH
#export PYSPARK_PYTHON=python3


saluation () {
   echo "--------------------------------------------------------"
   echo "Starting execution $1 (properties: $2)"
   echo "--------------------------------------------------------"

}

saluation "HdfsIn2HdfsOut1.py" $2
spark-submit --properties-file $2 HdfsIn2HdfsOut1.py

saluation "HdfsIn2HdfsOut2.py" $2
spark-submit --properties-file $2 HdfsIn2HdfsOut2.py

saluation "HdfsIn2HiveCreateTable.py" $2
spark-submit --properties-file $2 HdfsIn2HiveCreateTable.py

saluation "HdfsIn2HiveCreateInsertTable.py" $2
spark-submit --properties-file $2 HdfsIn2HiveCreateInsertTable.py

saluation "HiveInHiveOut.py" $2
spark-submit --properties-file $2 HiveInHiveOut.py

# Iceberg-on-Glue connection-instance scenario. Iceberg comes via --packages; the connection mapping
# (Glue catalog ARN -> platform_instance) is passed as --conf because the ARN's colons would be
# misparsed in a Spark properties file. moto's default account is 123456789012 / region us-east-1.
# This job uses the FILE emitter (overriding the REST emitter from the properties file) so the test
# can assert the emitted lineage edge directly; the output lands on a host-mounted dir. Spark lineage
# output carries per-run volatility (dataProcessInstance UUIDs, timestamps, temp paths), so a full
# golden is brittle here — test_e2e.py asserts the one stable signal: the glue,<instance> edge.
saluation "GlueIcebergConnectionInstance.py" $2
GLUE_ARN='arn:aws:glue:us-east-1:123456789012'
spark-submit --properties-file $2 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1 \
  --conf "spark.datahub.emitter=file" \
  --conf "spark.datahub.file.filename=/opt/workspace/glue-output/glue_mcps.json" \
  --conf "spark.datahub.metadata.dataset.connections.\"${GLUE_ARN}\".platformInstance=domain_a" \
  --conf "spark.datahub.metadata.dataset.connections.\"${GLUE_ARN}\".env=PROD" \
  GlueIcebergConnectionInstance.py


