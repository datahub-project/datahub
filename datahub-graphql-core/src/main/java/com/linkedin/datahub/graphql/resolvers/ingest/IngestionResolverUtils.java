package com.linkedin.datahub.graphql.resolvers.ingest;

import static com.linkedin.datahub.graphql.AcrylConstants.INGESTION_SOURCE_EXECUTOR_CLI;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_ENTITY_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.types.ingestion.IngestionSourceMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.RunInfo;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IngestionResolverUtils {

  private static final String DATABRICKS_INGESTION_SOURCE_TYPE = "databricks";
  private static final String UNITY_CATALOG_INGESTION_SOURCE_TYPE = "unity-catalog";
  private static final String HIVE_PLATFORM_URN = "urn:li:dataPlatform:hive";
  private static final String DATABRICKS_PLATFORM_URN = "urn:li:dataPlatform:databricks";

  @Nullable
  public static IngestionSource getIngestionSourceForEntity(
      @Nonnull final EntityClient entityClient,
      @Nonnull final QueryContext context,
      @Nonnull final Urn entityUrn,
      @Nullable final Set<String> aspectNames)
      throws Exception {
    // Fetch the aspects for the entity.
    final EntityResponse entityResponse =
        entityClient.getV2(
            context.getOperationContext(), entityUrn.getEntityType(), entityUrn, aspectNames);

    // Entity does not exist! Return no source.
    if (entityResponse == null) {
      return null;
    }

    // Get the latest "run id" for the entity
    final EnvelopedAspectMap aspectMap = entityResponse.getAspects();

    // https://linear.app/acryl-data/issue/OBS-56/collect-the-last-run-id-in-the-system-metadata-payload
    // this runId may or may not be the correct (last one) - so in some cases this
    // resolver may
    // return false incorrectly.
    final List<RunInfo> runs = SystemMetadataUtils.getLastIngestionRuns(aspectMap);

    if (runs.isEmpty()) {
      return null;
    }

    // For each run, try to link back to an ingestion source that produced it.
    for (RunInfo run : runs) {
      IngestionSource ingestionSource =
          tryGetIngestionSourceForRunId(entityClient, run.getId(), entityUrn, context);
      if (ingestionSource != null) {
        return ingestionSource;
      }
    }
    return null;
  }

  @Nullable
  private static IngestionSource tryGetIngestionSourceForRunId(
      @Nonnull final EntityClient entityClient,
      @Nonnull final String runId,
      @Nonnull final Urn entityUrn,
      @Nonnull final QueryContext context)
      throws Exception {

    final Urn runUrn =
        Urn.createFromString(String.format("urn:li:%s:", EXECUTION_REQUEST_ENTITY_NAME) + runId);

    final EntityResponse executionRequestEntityResponse =
        entityClient.getV2(
            context.getOperationContext(),
            Constants.EXECUTION_REQUEST_ENTITY_NAME,
            runUrn,
            Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME));

    // If no execution request, return null.
    if (executionRequestEntityResponse == null) {
      return null;
    }

    // Pull out the execution request source, which should have the ingestion source
    final EnvelopedAspectMap executionRequestAspects = executionRequestEntityResponse.getAspects();
    if (!executionRequestAspects.containsKey(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME)) {
      return null;
    }

    final EnvelopedAspect executionRequestEnvelopedInput =
        executionRequestAspects.get(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
    final ExecutionRequestInput executionRequestInput =
        new ExecutionRequestInput(executionRequestEnvelopedInput.getValue().data());
    final ExecutionRequestSource execRequestSource = executionRequestInput.getSource();

    // Get the ingestionSource entity to check for the source -> config ->
    // executorId.
    final EntityResponse ingestionSourceEntityResponse =
        entityClient.getV2(
            context.getOperationContext(),
            Constants.INGESTION_SOURCE_ENTITY_NAME,
            execRequestSource.getIngestionSource(),
            Collections.singleton(INGESTION_INFO_ASPECT_NAME));

    // If we cannot find the ingestion source, return null.
    if (ingestionSourceEntityResponse == null) {
      return null;
    }

    final EnvelopedAspectMap ingestionInfoAspects = ingestionSourceEntityResponse.getAspects();
    if (!ingestionInfoAspects.containsKey(Constants.INGESTION_INFO_ASPECT_NAME)) {
      return null;
    }

    final EnvelopedAspect ingestionSourceEnvelopedInfo =
        ingestionInfoAspects.get(Constants.INGESTION_INFO_ASPECT_NAME);
    final DataHubIngestionSourceInfo ingestionSourceInfo =
        new DataHubIngestionSourceInfo(ingestionSourceEnvelopedInfo.getValue().data());
    final DataHubIngestionSourceConfig ingestionSourceConfig = ingestionSourceInfo.getConfig();

    // executorId's from the CLI OR REMOTE executor are not valid because we don't
    // have auth info to
    // fetch data.
    final String executorId = ingestionSourceConfig.getExecutorId();
    final boolean isCorrectSource =
        isMatchingDataPlatform(entityUrn, ingestionSourceInfo.getType())
            && !INGESTION_SOURCE_EXECUTOR_CLI.equals(executorId);

    return isCorrectSource
        ? IngestionSourceMapper.map(context, ingestionSourceEntityResponse)
        : null;
  }

  private static Boolean isMatchingDataPlatform(
      @Nonnull final Urn entityUrn, @Nonnull final String type) {
    if (Constants.DATASET_ENTITY_NAME.equals(entityUrn.getEntityType())) {
      // it's possible that another ingestion source produced an aspect for the entity
      // in cases of
      // Datasets
      // e.g. for DBT. here we check the data platform urn and compare it to the
      // ingestion source
      // type.
      Urn dataPlatformUrn = UrnUtils.getUrn(entityUrn.getEntityKey().get(0));
      return type.equalsIgnoreCase(dataPlatformUrn.getId()) || isDatabricksEntity(entityUrn, type);
    }
    return true;
  }

  // For databricks entities, the data platform URN will not necessarily match the
  // ingestion source
  // type.
  private static boolean isDatabricksEntity(
      @Nonnull final Urn entityUrn, @Nonnull final String type) {
    return (type.equalsIgnoreCase(DATABRICKS_INGESTION_SOURCE_TYPE)
            || type.equalsIgnoreCase(UNITY_CATALOG_INGESTION_SOURCE_TYPE))
        && (entityUrn.toString().contains(DATABRICKS_PLATFORM_URN)
            || entityUrn.toString().contains(HIVE_PLATFORM_URN));
  }

  private IngestionResolverUtils() {}
}
