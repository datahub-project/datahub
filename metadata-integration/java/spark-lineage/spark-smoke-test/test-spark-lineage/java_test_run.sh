saluation () {
   echo "--------------------------------------------------------"
   echo "Starting exectuion"
   echo "--------------------------------------------------------"

}


saluation
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HdfsIn2HdfsOut1 build/libs/test-spark-lineage.jar 

saluation
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HdfsIn2HdfsOut2 build/libs/test-spark-lineage.jar 

saluation
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HdfsIn2HiveCreateTable build/libs/test-spark-lineage.jar 

saluation
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HdfsIn2HiveCreateInsertTable build/libs/test-spark-lineage.jar 

saluation
$1/bin/spark-submit --properties-file $2 --class test.spark.lineage.HiveInHiveOut build/libs/test-spark-lineage.jar 




