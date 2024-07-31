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




