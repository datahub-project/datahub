package datahub.spark.model.dataset;

import org.apache.hadoop.fs.Path;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;

import datahub.spark.model.LineageUtils;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class HdfsPathDataset implements SparkDataset {
  private final DatasetUrn urn;

  public HdfsPathDataset(Path path, String platformInstance, FabricType fabricType) {
    // TODO check static partitions?
    this(path.toUri().toString(), platformInstance, fabricType);
  }

  public HdfsPathDataset(String pathUri, String platformInstance, FabricType fabricType) {
    // TODO check static partitions?
    this.urn = LineageUtils.createDatasetUrn("hdfs", platformInstance, pathUri, fabricType);
  }

  @Override
  public DatasetUrn urn() {
    return this.urn;
  }
}
