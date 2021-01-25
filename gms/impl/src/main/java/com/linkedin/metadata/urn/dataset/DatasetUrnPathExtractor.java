package com.linkedin.metadata.urn.dataset;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.metadata.dao.scsi.UrnPathExtractor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public final class DatasetUrnPathExtractor implements UrnPathExtractor<DatasetUrn> {
  @Nonnull
  @Override
  public Map<String, Object> extractPaths(@Nonnull DatasetUrn urn) {
    return Collections.unmodifiableMap(new HashMap<String, String>() {
      {
        put("/platform", urn.getPlatformEntity().toString());
        put("/datasetName", urn.getDatasetNameEntity());
        put("/origin", urn.getOriginEntity().toString());
        put("/platform/platformName", urn.getPlatformEntity().getPlatformNameEntity());
      }
    });
  }
}
