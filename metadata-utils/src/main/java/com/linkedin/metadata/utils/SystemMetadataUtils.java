package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.SystemMetadata;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemMetadataUtils {

  private SystemMetadataUtils() {}

  public static SystemMetadata createDefaultSystemMetadata() {
    return new SystemMetadata()
        .setRunId(Constants.DEFAULT_RUN_ID)
        .setLastObserved(System.currentTimeMillis());
  }

  public static Long getLastIngested(@Nonnull EnvelopedAspectMap aspectMap) {
    Long lastIngested = null;
    for (String aspect : aspectMap.keySet()) {
      if (aspectMap.get(aspect).hasSystemMetadata()) {
        SystemMetadata systemMetadata = aspectMap.get(aspect).getSystemMetadata();
        if (systemMetadata.hasRunId()
            && !systemMetadata.getRunId().equals(DEFAULT_RUN_ID)
            && systemMetadata.hasLastObserved()) {
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
