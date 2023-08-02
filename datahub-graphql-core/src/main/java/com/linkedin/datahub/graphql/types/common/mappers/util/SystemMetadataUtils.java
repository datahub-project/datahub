package com.linkedin.datahub.graphql.types.common.mappers.util;

import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.mxe.SystemMetadata;

import javax.annotation.Nonnull;
import org.javatuples.Pair;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;

public class SystemMetadataUtils {

  private SystemMetadataUtils() {
  }

  private static Pair<Long, String> getLastIngestedData(@Nonnull EnvelopedAspectMap aspectMap) {
    Long lastIngested = null;
    String runId = null;

    for (String aspect : aspectMap.keySet()) {
      if (aspectMap.get(aspect).hasSystemMetadata()) {
        SystemMetadata systemMetadata = aspectMap.get(aspect).getSystemMetadata();
        if (systemMetadata.hasRunId() && !systemMetadata.getRunId().equals(DEFAULT_RUN_ID) && systemMetadata.hasLastObserved()) {
          Long lastObserved = systemMetadata.getLastObserved();
          if (lastIngested == null || lastObserved > lastIngested) {
            lastIngested = lastObserved;
            runId = systemMetadata.getRunId();
          }
        }
      }
    }
    Pair<Long, String> lastIngestedData = new Pair<Long, String>(lastIngested, runId);
    return lastIngestedData;
  }

  public static Long getLastIngested(@Nonnull EnvelopedAspectMap aspectMap) {
    Pair<Long, String> lastIngestedData = getLastIngestedData(aspectMap);
    return lastIngestedData.getValue0();
  }

  public static String getLastIngestedRunId(@Nonnull EnvelopedAspectMap aspectMap) {
    Pair<Long, String> lastIngestedData = getLastIngestedData(aspectMap);
    return lastIngestedData.getValue1();
  }
}
