package com.linkedin.datahub.graphql.resolvers.connection;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Entity;
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

import com.linkedin.metadata.Constants;
import com.linkedin.datahub.graphql.QueryContext;

import static com.linkedin.datahub.graphql.AcrylConstants.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;


/**
 * Resolver responsible for returning whether a valid connection exists for an entity
 */
@Slf4j
public class ConnectionForEntityExistsResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityClient _entityClient;
  private final Cache<Urn, Boolean> _connectionCache;
  private static final String REMOTE_EXECUTOR_ID = "remote";

  public ConnectionForEntityExistsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
    _connectionCache = CacheBuilder.newBuilder()
        .expireAfterWrite(60, TimeUnit.MINUTES)
        .build();
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    String entityUrnString = environment.getArgument("urn");
    if (entityUrnString == null && environment.getSource() != null) {
      entityUrnString = ((Entity) environment.getSource()).getUrn();
    }
    Objects.requireNonNull(entityUrnString, "Entity urn must not be null!");

    final Urn entityUrn = Urn.createFromString(entityUrnString);

    return CompletableFuture.supplyAsync(() -> {
      try {
        final Boolean cachedConnection = _connectionCache.getIfPresent(entityUrn);
        if (cachedConnection != null) {
          return cachedConnection;
        }

        // fetch the entity for this urn
        final EntityResponse entityResponse = _entityClient.getV2(
            entityUrn.getEntityType(),
            entityUrn,
            null,
            context.getAuthentication()
        );
        if (entityResponse == null) {
            _connectionCache.put(entityUrn, false);
            return false;
        }

        // Get the latest "run id" for the entity
        final EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        // https://linear.app/acryl-data/issue/OBS-56/collect-the-last-run-id-in-the-system-metadata-payload
        // this runId may or may not be the correct (last one) - so in some cases this resolver may return false incorrectly.
        final String runId = SystemMetadataUtils.getLastIngestedRunId(aspectMap);
        if (runId == null) {
            _connectionCache.put(entityUrn, false);
            return false;
        }

        // Check for a dataHubExecutionRequest entity that has this run id
        final Urn runUrn = Urn.createFromString("urn:li:dataHubExecutionRequest:" + runId);
        final EntityResponse executionRequestEntityResponse = _entityClient.getV2(
            Constants.EXECUTION_REQUEST_ENTITY_NAME,
            runUrn,
            Collections.singleton(EXECUTION_REQUEST_INPUT_ASPECT_NAME),
            context.getAuthentication()
        );
        if (executionRequestEntityResponse == null) {
            _connectionCache.put(entityUrn, false);
            return false;
        }

        // Pull out the execution request source, which should have the ingestion source
        final EnvelopedAspectMap executionRequestAspects = executionRequestEntityResponse.getAspects();
        if (executionRequestAspects == null || !executionRequestAspects.containsKey(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME)) {
            _connectionCache.put(entityUrn, false);
            return false;
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
        if (ingestionSourceEntityResponse == null) {
            _connectionCache.put(entityUrn, false);
            return false;
        }

        final EnvelopedAspectMap ingestionInfoAspects = ingestionSourceEntityResponse.getAspects();
        if (ingestionInfoAspects == null) {
            _connectionCache.put(entityUrn, false);
            return false;
        }

        final EnvelopedAspect ingestionSourceEnvelopedInfo = ingestionInfoAspects.get(Constants.INGESTION_INFO_ASPECT_NAME);
        if (ingestionSourceEnvelopedInfo == null) {
            _connectionCache.put(entityUrn, false);
            return false;
        }

        final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(ingestionSourceEnvelopedInfo.getValue().data());
        final DataHubIngestionSourceConfig ingestionSourceConfig = ingestionSourceInfo.getConfig();
        if (ingestionSourceConfig == null) {
            _connectionCache.put(entityUrn, false);
            return false;
        }

        // executorId's from the CLI are not valid because we don't have auth info to fetch data.
        final String executorId = ingestionSourceConfig.getExecutorId();
        final boolean connectionExists = !INGESTION_SOURCE_EXECUTOR_CLI.equals(executorId) && !REMOTE_EXECUTOR_ID.equals(executorId);
        _connectionCache.put(entityUrn, connectionExists);
        return connectionExists;
      } catch (Exception e) {
        log.error(String.format("Failed to check whether connection for entity %s exists", entityUrn.toString()), e);
        return false;
      }
    });
  }
}
