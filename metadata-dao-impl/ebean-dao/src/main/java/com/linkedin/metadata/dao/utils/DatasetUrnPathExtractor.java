package com.linkedin.metadata.dao.utils;

import com.linkedin.common.urn.DatasetUrn;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


public class DatasetUrnPathExtractor implements UrnPathExtractor<DatasetUrn> {
  @Override
  public Map<String, Object> extractPaths(@Nonnull DatasetUrn urn) {
    return Collections.unmodifiableMap(new HashMap() {
      {
        put("/platform", urn.getPlatformEntity().toString());
        put("/datasetName", urn.getDatasetNameEntity());
        put("/origin", urn.getOriginEntity().toString());
        put("/platform/platformName", urn.getPlatformEntity().getPlatformNameEntity());
      }
    });
  }
}
