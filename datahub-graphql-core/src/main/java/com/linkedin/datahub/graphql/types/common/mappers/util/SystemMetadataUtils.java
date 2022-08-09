package com.linkedin.datahub.graphql.types.common.mappers.util;

import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.mxe.SystemMetadata;

import javax.annotation.Nonnull;
import java.util.Set;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;

public class SystemMetadataUtils {

  private SystemMetadataUtils() {
  }

  public static Long getLastIngested(@Nonnull Set<String> aspects, @Nonnull EnvelopedAspectMap aspectMap) {
    Long lastIngested = null;
    for (String aspect : aspects) {
      if (aspectMap.containsKey(aspect) && aspectMap.get(aspect).hasSystemMetadata()) {
        SystemMetadata systemMetadata = aspectMap.get(aspect).getSystemMetadata();
        if (systemMetadata.hasRunId() && !systemMetadata.getRunId().equals(DEFAULT_RUN_ID) && systemMetadata.hasLastObserved()) {
          Long lastObserved = systemMetadata.getLastObserved();
          if (lastIngested == null || lastObserved > lastIngested) {
            lastIngested = lastObserved;
          }
        }
      }
    }
    return lastIngested;
  }
}
