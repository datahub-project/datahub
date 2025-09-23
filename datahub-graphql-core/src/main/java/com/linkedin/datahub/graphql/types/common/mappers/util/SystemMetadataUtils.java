package com.linkedin.datahub.graphql.types.common.mappers.util;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;

import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.mxe.SystemMetadata;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SystemMetadataUtils {

  private SystemMetadataUtils() {}

  @Nullable
  public static Long getLastIngestedTime(@Nonnull EnvelopedAspectMap aspectMap) {
    return getLastIngestionRun(aspectMap).map(RunInfo::getTime).orElse(null);
  }

  @Nullable
  public static String getLastIngestedRunId(@Nonnull EnvelopedAspectMap aspectMap) {
    return getLastIngestionRun(aspectMap).map(RunInfo::getId).orElse(null);
  }

  /**
   * Returns the most recent ingestion run based on the most recent aspects present for the entity.
   */
  @Nonnull
  private static Optional<RunInfo> getLastIngestionRun(@Nonnull EnvelopedAspectMap aspectMap) {
    return aspectMap.values().stream()
        .filter(EnvelopedAspect::hasSystemMetadata)
        .map(EnvelopedAspect::getSystemMetadata)
        .filter(SystemMetadata::hasLastObserved)
        .map(
            systemMetadata ->
                Optional.ofNullable(systemMetadata.getLastRunId())
                    .filter(lastRunId -> !lastRunId.equals(DEFAULT_RUN_ID))
                    .or(
                        () ->
                            Optional.ofNullable(systemMetadata.getRunId())
                                .filter(runId -> !runId.equals(DEFAULT_RUN_ID)))
                    .map(runId -> new RunInfo(runId, systemMetadata.getLastObserved()))
                    .orElse(null))
        .filter(Objects::nonNull)
        .max(Comparator.comparingLong(RunInfo::getTime));
  }
}
