package test.spark.lineage;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveInHiveOut {

  private static final String TEST_NAME = "Java" + HiveInHiveOut.class.getSimpleName();
  private static final String DATA_DIR = "../resources/data";

  public static void main(String[] args) {
    SparkSession spark =
        SparkSession.builder().appName(TEST_NAME).enableHiveSupport().getOrCreate();

    spark.sql("DROP DATABASE IF EXISTS " + TEST_NAME + " CASCADE");
    spark.sql("CREATE DATABASE IF NOT EXISTS " + TEST_NAME);
    spark.sql("DROP TABLE IF EXISTS " + Utils.tbl(TEST_NAME, "hivetab"));
    spark.sql("DROP TABLE IF EXISTS " + Utils.tbl(TEST_NAME, "foo5"));

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

    df1.createOrReplaceTempView("v1");
    df2.createOrReplaceTempView("v2");

    // CreateHiveTableAsSelectCommand
    spark.sql(
        "create table "
            + Utils.tbl(TEST_NAME, "foo5")
            + " as "
            + "(select v1.a, v1.b, v2.c, v2.d from v1 join v2 on v1.id = v2.id)");

    // CreateHiveTableAsSelectCommand
    spark.sql(
        "create table "
            + Utils.tbl(TEST_NAME, "hivetab")
            + " as "
            + "(select * from "
            + Utils.tbl(TEST_NAME, "foo5")
            + ")");

    // InsertIntoHiveTable
    spark.sql(
        "insert into "
            + Utils.tbl(TEST_NAME, "hivetab")
            + " (select * from "
            + Utils.tbl(TEST_NAME, "foo5")
            + ")");

    Dataset<Row> df = spark.sql("select * from " + Utils.tbl(TEST_NAME, "foo5"));

    // InsertIntoHiveTable
    df.write().insertInto(Utils.tbl(TEST_NAME, "hivetab"));

    spark.stop();
  }
}
