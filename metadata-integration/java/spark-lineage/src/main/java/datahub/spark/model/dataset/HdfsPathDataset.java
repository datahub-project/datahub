package datahub.spark.model.dataset;

import org.apache.hadoop.fs.Path;

import com.linkedin.common.FabricType;

import lombok.ToString;

@ToString
public class HdfsPathDataset extends SparkDataset {
  
  public HdfsPathDataset(Path path, String platformInstance, FabricType fabricType) {
    // TODO check static partitions?
    this(path.toUri().toString(), platformInstance, fabricType);
  }

  public HdfsPathDataset(String pathUri, String platformInstance, FabricType fabricType) {
    // TODO check static partitions?
    super("hdfs", platformInstance, pathUri, fabricType);
  }

}
