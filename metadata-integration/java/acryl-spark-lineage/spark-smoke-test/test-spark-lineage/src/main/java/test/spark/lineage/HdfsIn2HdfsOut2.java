package test.spark.lineage;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class HdfsIn2HdfsOut2 {

  private static final String TEST_NAME = "Java" + HdfsIn2HdfsOut2.class.getSimpleName();
  private static final String DATA_DIR = "../resources/data";

  public static void main(String[] args) {
    SparkSession spark =
        SparkSession.builder().appName(TEST_NAME).enableHiveSupport().getOrCreate();

    Dataset<Row> df1 = spark.read().option("header", "true").csv(DATA_DIR + "/in1.csv");
    Dataset<Row> df2 = spark.read().option("header", "true").csv(DATA_DIR + "/in2.csv");
    df1.createOrReplaceTempView("v1");
    df2.createOrReplaceTempView("v2");

    Dataset<Row> df =
        spark.sql(
            "select v1.c1 as a, v1.c2 as b, v2.c1 as c, v2.c2 as d from v1 join v2 on v1.id = v2.id");

    // InsertIntoHadoopFsRelationCommand
    df.write().mode(SaveMode.Overwrite).csv(DATA_DIR + "/" + TEST_NAME + "/out.csv");

    Dataset<Row> dfO =
        spark.sql(
            "select v1.c1 as a1, v1.c2 as b1, v2.c1 as c1, v2.c2 as d1 from v1 join v2 on v1.id = v2.id");

    // InsertIntoHadoopFsRelationCommand
    dfO.write().mode(SaveMode.Overwrite).csv(DATA_DIR + "/" + TEST_NAME + "/out.csv");
    spark.stop();
  }
}
