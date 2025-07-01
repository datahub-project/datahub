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


