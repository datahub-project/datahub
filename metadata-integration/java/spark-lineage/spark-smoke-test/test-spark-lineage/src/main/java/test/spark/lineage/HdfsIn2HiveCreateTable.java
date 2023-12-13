package test.spark.lineage;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class HdfsIn2HiveCreateTable {

  private static final String TEST_NAME = "Java" + HdfsIn2HiveCreateTable.class.getSimpleName();
  private static final String DATA_DIR = "../resources/data";

  public static void main(String[] args) {
    SparkSession spark =
        SparkSession.builder().appName(TEST_NAME).enableHiveSupport().getOrCreate();

    spark.sql("DROP DATABASE IF EXISTS " + TEST_NAME);
    spark.sql("CREATE DATABASE IF NOT EXISTS " + TEST_NAME);
    spark.sql("DROP TABLE IF EXISTS " + Utils.tbl(TEST_NAME, "foo3"));

    Dataset<Row> df1 =
        spark
            .read()
            .option("header", "true")
            .csv(DATA_DIR + "/in1.csv")
            .withColumnRenamed("c1", "a")
            .withColumnRenamed("c2", "b");

    Dataset<Row> df2 =
        spark
            .read()
            .option("header", "true")
            .csv(DATA_DIR + "/in2.csv")
            .withColumnRenamed("c1", "c")
            .withColumnRenamed("c2", "d");

    Dataset<Row> df = df1.join(df2, "id").drop("id");

    df.write()
        .mode(SaveMode.Overwrite)
        .saveAsTable(Utils.tbl(TEST_NAME, "foo3")); // CreateDataSourceTableAsSelectCommand

    spark.stop();
  }
}
