package com.linkedin.datahub.graphql.resolvers.ingest.source;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionResolverUtils;
import com.linkedin.datahub.graphql.types.common.mappers.util.RunInfo;
import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import com.linkedin.metadata.Constants;
import com.linkedin.datahub.graphql.QueryContext;

import static com.linkedin.datahub.graphql.AcrylConstants.*;
import static com.linkedin.metadata.Constants.*;


/**
 * Resolver responsible for fetching the ingestion source responsible for ingesting a particular entity.
 *
 * Ideally, in the future we leverage a pipeline_name which is placed in the system metadata to do this.
 * For now, we rely on fetching the "last run id" for an entity and backtracing to the ingestion source which produced
 * it.
 */
@Slf4j
public class IngestionSourceForEntityResolver implements DataFetcher<CompletableFuture<IngestionSource>> {
  private final EntityClient _entityClient;
  private final Cache<Urn, IngestionSource> _ingestionSourceCache;
  private static final String REMOTE_EXECUTOR_ID = "remote";

  public IngestionSourceForEntityResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = entityClient;
    _ingestionSourceCache = CacheBuilder.newBuilder()
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .build();
  }

  @Override
  public CompletableFuture<IngestionSource> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    String entityUrnString = environment.getArgument("urn");
    if (entityUrnString == null && environment.getSource() != null) {
      entityUrnString = ((Entity) environment.getSource()).getUrn();
    }
    Objects.requireNonNull(entityUrnString, "Entity urn must not be null!");

    final Urn entityUrn = Urn.createFromString(entityUrnString);

    return CompletableFuture.supplyAsync(() -> {
      try {
        final IngestionSource cachedSource = _ingestionSourceCache.getIfPresent(entityUrn);
        if (cachedSource != null) {
          return cachedSource;
        }
        final IngestionSource maybeSource = getIngestionSourceForEntity(entityUrn, context);
        if (maybeSource != null) {
          _ingestionSourceCache.put(entityUrn, maybeSource);
        }
        return maybeSource;
      } catch (Exception e) {
        log.error(String.format("Failed to check whether ingestion source for entity %s exists", entityUrn.toString()), e);
        return null;
      }
    });
  }

  @Nullable
  private IngestionSource getIngestionSourceForEntity(@Nonnull final Urn entityUrn, @Nonnull final QueryContext context) throws Exception {
    // Fetch the aspects for the entity.
    final EntityResponse entityResponse = _entityClient.getV2(
        entityUrn.getEntityType(),
        entityUrn,
        null,
        context.getAuthentication()
    );

    // Entity does not exist! Return no source.
    if (entityResponse == null) {
      return null;
    }

    // Get the latest "run id" for the entity
    final EnvelopedAspectMap aspectMap = entityResponse.getAspects();

    // https://linear.app/acryl-data/issue/OBS-56/collect-the-last-run-id-in-the-system-metadata-payload
    // this runId may or may not be the correct (last one) - so in some cases this resolver may return false incorrectly.
    final List<RunInfo> runs = SystemMetadataUtils.getLastIngestionRuns(aspectMap);

    if (runs.isEmpty()) {
      return null;
    }

    // For each run, try to link back to an ingestion source that produced it.
    for (RunInfo run : runs) {
     IngestionSource ingestionSource = tryGetIngestionSourceForRunId(run.getId(), entityUrn, context);
     if (ingestionSource != null) {
       return ingestionSource;
     }
    }
    return null;
  }

  @Nullable
  private IngestionSource tryGetIngestionSourceForRunId(
      @Nonnull final String runId,
      @Nonnull final Urn entityUrn,
      @Nonnull final QueryContext context) throws Exception {

    final Urn runUrn = Urn.createFromString(String.format("urn:li:%s:", EXECUTION_REQUEST_ENTITY_NAME) + runId);

    final EntityResponse executionRequestEntityResponse = _entityClient.getV2(
        Constants.EXECUTION_REQUEST_ENTITY_NAME,
        runUrn,
        Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME),
        context.getAuthentication()
    );

    // If no execution request, return null.
    if (executionRequestEntityResponse == null) {
      return null;
    }

    // Pull out the execution request source, which should have the ingestion source
    final EnvelopedAspectMap executionRequestAspects = executionRequestEntityResponse.getAspects();
    if (!executionRequestAspects.containsKey(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME)) {
      return null;
    }

    final EnvelopedAspect executionRequestEnvelopedInput = executionRequestAspects.get(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
    final ExecutionRequestInput executionRequestInput = new ExecutionRequestInput(executionRequestEnvelopedInput.getValue().data());
    final ExecutionRequestSource execRequestSource = executionRequestInput.getSource();

    // Get the ingestionSource entity to check for the source -> config -> executorId.
    final EntityResponse ingestionSourceEntityResponse = _entityClient.getV2(
        Constants.INGESTION_SOURCE_ENTITY_NAME,
        execRequestSource.getIngestionSource(),
        Collections.singleton(INGESTION_INFO_ASPECT_NAME),
        context.getAuthentication()
    );

    // If we cannot find the ingestion source, return null.
    if (ingestionSourceEntityResponse == null) {
      return null;
    }

    System.out.println(ingestionSourceEntityResponse);

    final EnvelopedAspectMap ingestionInfoAspects = ingestionSourceEntityResponse.getAspects();
    if (!ingestionInfoAspects.containsKey(Constants.INGESTION_INFO_ASPECT_NAME)) {
      return null;
    }

    final EnvelopedAspect ingestionSourceEnvelopedInfo = ingestionInfoAspects.get(Constants.INGESTION_INFO_ASPECT_NAME);
    final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(ingestionSourceEnvelopedInfo.getValue().data());
    final DataHubIngestionSourceConfig ingestionSourceConfig = ingestionSourceInfo.getConfig();

    // executorId's from the CLI OR REMOTE executor are not valid because we don't have auth info to fetch data.
    // Todo: Remove check 2 once we support remote monitors!!!
    final String executorId = ingestionSourceConfig.getExecutorId();
    final boolean isCorrectSource = isMatchingDataPlatform(entityUrn, ingestionSourceInfo.getType())
        && !INGESTION_SOURCE_EXECUTOR_CLI.equals(executorId) && !REMOTE_EXECUTOR_ID.equals(executorId);

    return isCorrectSource ? IngestionResolverUtils.mapIngestionSource(ingestionSourceEntityResponse) : null;
  }

  private Boolean isMatchingDataPlatform(@Nonnull final Urn entityUrn, @Nonnull final String type) {
    if (Constants.DATASET_ENTITY_NAME.equals(entityUrn.getEntityType())) {
      // it's possible that another ingestion source produced an aspect for the entity in cases of Datasets
      // e.g. for DBT. here we check the data platform urn and compare it to the ingestion source type.
      Urn dataPlatformUrn = UrnUtils.getUrn(entityUrn.getEntityKey().get(0));
      return type.equalsIgnoreCase(dataPlatformUrn.getId());
    }
    return true;
  }
}
