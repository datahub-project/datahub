package datahub.spark.model.dataset;

import org.apache.hadoop.fs.Path;

public class HdfsDatasetFactory {

  private HdfsDatasetFactory() {

  }

  public static SparkDataset createDataset(Path path) throws InstantiationException {
    String pathUri = path.toUri().toString();
    if (S3Dataset.isMatch(pathUri)) {
      return S3Dataset.create(pathUri);
    } else {
      return createHdfsDataset(pathUri);
    }
  }

  private static SparkDataset createHdfsDataset(String pathUri) {
    return new HdfsPathDataset(pathUri);
  }

}
