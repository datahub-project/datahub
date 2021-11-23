package com.linkedin.datahub.graphql.resolvers.ingest;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.IngestionConfig;
import com.linkedin.datahub.graphql.generated.IngestionRecipe;
import com.linkedin.datahub.graphql.generated.IngestionSchedule;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.IngestionSourceType;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesInput;
import com.linkedin.datahub.graphql.generated.ListIngestionSourcesResult;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceRecipe;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.ListResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

public class ListIngestionSourcesResolver implements DataFetcher<CompletableFuture<ListIngestionSourcesResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final EntityClient _entityClient;

  public ListIngestionSourcesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListIngestionSourcesResult> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (IngestionAuthUtils.canManageIngestion(context)) {
      final ListIngestionSourcesInput input = bindArgument(environment.getArgument("input"), ListIngestionSourcesInput.class);
      final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
      final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();

      return CompletableFuture.supplyAsync(() -> {
        try {
          // First, get all ingestion sources Urns.
          final ListResult gmsResult = _entityClient.list(Constants.INGESTION_SOURCE_ENTITY_NAME, Collections.emptyMap(), start, count, context.getActor());

          // Then, resolve all ingestion sources
          final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(Constants.INGESTION_SOURCE_ENTITY_NAME, new HashSet<>(gmsResult.getEntities()), context.getActor());

          // Now that we have entities we can bind this to a result.
          final ListIngestionSourcesResult result = new ListIngestionSourcesResult();
          result.setStart(gmsResult.getStart());
          result.setCount(gmsResult.getCount());
          result.setTotal(gmsResult.getTotal());
          result.setIngestionSources(mapEntities(entities.values()));
          return result;

        } catch (Exception e) {
          throw new RuntimeException("Failed to list ingestion sources", e);
        }
      });
    }
    throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
  }

  private List<IngestionSource> mapEntities(final Collection<EntityResponse> entities) {
    final List<IngestionSource> results = new ArrayList<>();
    for (EntityResponse response : entities) {
      final Urn entityUrn = response.getUrn();
      final EnvelopedAspectMap aspects = response.getAspects();

      // There should ALWAYS be an info aspect.
      final EnvelopedAspect envelopedInfo = aspects.get(Constants.INGESTION_INFO_ASPECT_NAME);

      // Bind into a strongly typed object.
      final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());

      // Map using the strongly typed object.
      results.add(mapIngestionSourceInfo(entityUrn, ingestionSourceInfo));
    }
    return results;
  }

  private IngestionSource mapIngestionSourceInfo(final Urn urn, final DataHubIngestionSourceInfo info) {
    final IngestionSource result = new IngestionSource();
    result.setUrn(urn.toString());
    result.setDisplayName(info.getDisplayName());
    result.setType(IngestionSourceType.valueOf(info.getType()));
    result.setConfig(mapIngestionSourceConfig(info.getConfig()));
    if (info.hasSchedule()) {
      result.setSchedule(mapIngestionSourceSchedule(info.getSchedule()));
    }
    return result;
  }

  private IngestionConfig mapIngestionSourceConfig(final DataHubIngestionSourceConfig config) {
    final IngestionConfig result = new IngestionConfig();
    if (config.hasRecipe()) {
      result.setRecipe(mapIngestionSourceRecipe(config.getRecipe()));
    }
    return result;
  }

  private IngestionRecipe mapIngestionSourceRecipe(final DataHubIngestionSourceRecipe recipe) {
    final IngestionRecipe result = new IngestionRecipe();
    result.setJson(recipe.getJson());
    return result;
  }

  private IngestionSchedule mapIngestionSourceSchedule(final DataHubIngestionSourceSchedule schedule) {
    final IngestionSchedule result = new IngestionSchedule();
    result.setInterval(schedule.getInterval());
    result.setStartTimeMs(schedule.getStartTimeMs());
    result.setEndTimeMs(schedule.getEndTimeMs());
    return result;
  }
}