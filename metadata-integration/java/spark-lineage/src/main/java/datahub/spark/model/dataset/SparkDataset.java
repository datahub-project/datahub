package datahub.spark.model.dataset;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;

import datahub.spark.SparkConfigUtil;
import datahub.spark.model.LineageUtils;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public abstract class SparkDataset {

  private DatasetUrn urn;

  public SparkDataset(String platform, String platformInstance, String name, FabricType fabricType) {
    super();
    this.urn = LineageUtils.createDatasetUrn(platform, platformInstance, name, fabricType);
  }

  public SparkDataset(String platform, String name) {
    super();
    this.urn = LineageUtils.createDatasetUrn(platform, SparkConfigUtil.getCommonPlatformInstance(), name,
        SparkConfigUtil.getCommonFabricType());
  }

  public DatasetUrn urn() {
    return urn;
  }
}
