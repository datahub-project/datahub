package datahub.spark.model.dataset;

import org.apache.hadoop.fs.Path;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class HdfsPathDataset implements SparkDataset {
  private final DatasetUrn urn;

  public HdfsPathDataset(Path path, FabricType fabricType) {
    // TODO check static partitions?
    this(path.toUri().toString(), fabricType);
  }

  public HdfsPathDataset(String pathUri, FabricType fabricType) {
    // TODO check static partitions?
    this.urn = new DatasetUrn(new DataPlatformUrn("hdfs"), pathUri, fabricType);
  }

  @Override
  public DatasetUrn urn() {
    return this.urn;
  }
}
