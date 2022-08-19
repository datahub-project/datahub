package datahub.spark.model.dataset;

public class BigqueryDataset extends SparkDataset {

  public BigqueryDataset(String name) {
    super("bigquery", name);
  }

}
