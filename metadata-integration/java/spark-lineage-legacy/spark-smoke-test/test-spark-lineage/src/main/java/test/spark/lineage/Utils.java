package test.spark.lineage;

public class Utils {
  public static String tbl(String testDb, String tbl) {
    return testDb + "." + tbl;
  }
}
