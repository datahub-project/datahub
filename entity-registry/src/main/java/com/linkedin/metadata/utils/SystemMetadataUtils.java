package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.mxe.SystemMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SystemMetadataUtils {

  private static final Set<String> LAST_INGESTED_ALLOWED_ASPECTS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  DATASET_PROPERTIES_ASPECT_NAME,
                  SCHEMA_METADATA_ASPECT_NAME,
                  CONTAINER_PROPERTIES_ASPECT_NAME,
                  NOTEBOOK_INFO_ASPECT_NAME,
                  DASHBOARD_INFO_ASPECT_NAME,
                  CHART_INFO_ASPECT_NAME,
                  DATA_FLOW_INFO_ASPECT_NAME,
                  DATA_JOB_INFO_ASPECT_NAME,
                  DATA_JOB_INPUT_OUTPUT_ASPECT_NAME,
                  DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME,
                  ML_MODEL_PROPERTIES_ASPECT_NAME,
                  ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME,
                  ML_PRIMARY_KEY_PROPERTIES_ASPECT_NAME,
                  ML_FEATURE_TABLE_PROPERTIES_ASPECT_NAME,
                  ML_FEATURE_PROPERTIES_ASPECT_NAME,
                  SCHEMA_FIELD_INFO_ASPECT_NAME,
                  QUERY_PROPERTIES_ASPECT_NAME,
                  PLATFORM_RESOURCE_INFO_ASPECT_NAME,
                  SIBLINGS_ASPECT_NAME,
                  GLOSSARY_TERM_INFO_ASPECT_NAME,
                  STATUS_ASPECT_NAME,
                  BROWSE_PATHS_V2_ASPECT_NAME,
                  DATASET_PROFILE_ASPECT_NAME)));

  private SystemMetadataUtils() {}

  public static SystemMetadata createDefaultSystemMetadata() {
    return generateSystemMetadataIfEmpty(null);
  }

  public static SystemMetadata createDefaultSystemMetadata(@Nullable String runId) {
    return generateSystemMetadataIfEmpty(
        new SystemMetadata()
            .setRunId(runId, SetMode.REMOVE_IF_NULL)
            .setLastObserved(System.currentTimeMillis()));
  }

  public static SystemMetadata generateSystemMetadataIfEmpty(
      @Nullable SystemMetadata systemMetadata) {
    SystemMetadata result = systemMetadata == null ? new SystemMetadata() : systemMetadata;
    if (result.getRunId() == null) {
      result.setRunId(DEFAULT_RUN_ID);
    }
    if (!result.hasLastObserved() || result.getLastObserved() == 0) {
      result.setLastObserved(System.currentTimeMillis());
    }
    return result;
  }

  @Nullable
  public static Long lastIngestedTime(@Nonnull EnvelopedAspectMap aspectMap) {
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
      // Skip aspects that are not allowed to represent the "last synchronized time".
      if (!LAST_INGESTED_ALLOWED_ASPECTS.contains(aspect)) {
        continue;
      }

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

  public static SystemMetadata parseSystemMetadata(String jsonSystemMetadata) {
    if (jsonSystemMetadata == null || jsonSystemMetadata.equals("")) {
      return createDefaultSystemMetadata();
    }
    return RecordUtils.toRecordTemplate(SystemMetadata.class, jsonSystemMetadata);
  }
}
