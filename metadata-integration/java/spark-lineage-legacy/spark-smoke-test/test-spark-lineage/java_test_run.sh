# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

saluation () {
   echo "--------------------------------------------------------"
   echo "Starting execution $1"
   echo "--------------------------------------------------------"

}


saluation "test.spark.lineage.HdfsIn2HdfsOut1"
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HdfsIn2HdfsOut1 build/libs/test-spark-lineage.jar 

saluation "test.spark.lineage.HdfsIn2HdfsOut2"
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HdfsIn2HdfsOut2 build/libs/test-spark-lineage.jar 

saluation "test.spark.lineage.HdfsIn2HiveCreateTable"
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HdfsIn2HiveCreateTable build/libs/test-spark-lineage.jar 

saluation "test.spark.lineage.HdfsIn2HiveCreateInsertTable"
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HdfsIn2HiveCreateInsertTable build/libs/test-spark-lineage.jar 

saluation "test.spark.lineage.HiveInHiveOut"
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HiveInHiveOut build/libs/test-spark-lineage.jar 




