package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.SystemMetadata;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

  public static SystemMetadata generateSystemMetadataIfEmpty(
      @Nullable SystemMetadata systemMetadata) {
    return systemMetadata == null ? createDefaultSystemMetadata() : systemMetadata;
  }

  @Nullable
  public static Long getLastIngestedTime(@Nonnull EnvelopedAspectMap aspectMap) {
    RunInfo lastIngestionRun = getLastIngestionRun(aspectMap);
    return lastIngestionRun != null ? lastIngestionRun.getTime() : null;
  }

  @Nullable
  public static String getLastIngestedRunId(@Nonnull EnvelopedAspectMap aspectMap) {
    RunInfo lastIngestionRun = getLastIngestionRun(aspectMap);
    return lastIngestionRun != null ? lastIngestionRun.getId() : null;
  }

  /**
   * Returns a sorted list of all of the most recent ingestion runs based on the most recent aspects
   * present for the entity.
   */
  @Nonnull
  public static List<RunInfo> getLastIngestionRuns(@Nonnull EnvelopedAspectMap aspectMap) {
    final List<RunInfo> runs = new ArrayList<>();
    for (String aspect : aspectMap.keySet()) {
      if (aspectMap.get(aspect).hasSystemMetadata()) {
        SystemMetadata systemMetadata = aspectMap.get(aspect).getSystemMetadata();
        if (systemMetadata.hasLastRunId()
            && !systemMetadata.getLastRunId().equals(DEFAULT_RUN_ID)
            && systemMetadata.hasLastObserved()) {
          Long lastObserved = systemMetadata.getLastObserved();
          String runId = systemMetadata.getLastRunId();
          RunInfo run = new RunInfo(runId, lastObserved);
          runs.add(run);
        } else if (systemMetadata.hasRunId()
            && !systemMetadata.getRunId().equals(DEFAULT_RUN_ID)
            && systemMetadata.hasLastObserved()) {
          // Handle the legacy case: Check original run ids.
          Long lastObserved = systemMetadata.getLastObserved();
          String runId = systemMetadata.getRunId();
          RunInfo run = new RunInfo(runId, lastObserved);
          runs.add(run);
        }
      }
    }
    runs.sort((a, b) -> Long.compare(b.getTime(), a.getTime()));
    return runs;
  }

  @Nullable
  private static RunInfo getLastIngestionRun(@Nonnull EnvelopedAspectMap aspectMap) {
    List<RunInfo> runs = getLastIngestionRuns(aspectMap);
    return !runs.isEmpty() ? runs.get(0) : null; // Just take the first, to get the most recent run.
  }
}
