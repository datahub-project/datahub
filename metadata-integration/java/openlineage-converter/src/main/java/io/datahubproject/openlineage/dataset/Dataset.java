package io.datahubproject.openlineage.dataset;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DatasetUrn;
import io.datahubproject.openlineage.config.DatahubOpenlineageConfig;
import io.datahubproject.openlineage.utils.DatahubUtils;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public abstract class Dataset {

  private final DatasetUrn urn;

  public Dataset(String platform, String platformInstance, String name, FabricType fabricType) {
    super();
    this.urn = DatahubUtils.createDatasetUrn(platform, platformInstance, name, fabricType);
  }

  public Dataset(String platform, String name, DatahubOpenlineageConfig datahubConfig) {
    super();
    this.urn =
        DatahubUtils.createDatasetUrn(
            platform, datahubConfig.getPlatformInstance(), name, datahubConfig.getFabricType());
  }

  public DatasetUrn urn() {
    return urn;
  }
}
