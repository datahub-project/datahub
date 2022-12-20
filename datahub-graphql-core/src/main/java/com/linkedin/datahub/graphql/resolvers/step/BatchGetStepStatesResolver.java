package com.linkedin.datahub.graphql.resolvers.step;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchGetStepStatesInput;
import com.linkedin.datahub.graphql.generated.BatchGetStepStatesResult;
import com.linkedin.datahub.graphql.generated.StepStateResult;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.DataHubStepStateKey;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.step.DataHubStepStateProperties;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.EntityKeyUtils.*;


@Slf4j
@RequiredArgsConstructor
public class BatchGetStepStatesResolver implements DataFetcher<CompletableFuture<BatchGetStepStatesResult>> {
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<BatchGetStepStatesResult> get(@Nonnull final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final BatchGetStepStatesInput input =
        bindArgument(environment.getArgument("input"), BatchGetStepStatesInput.class);

    return CompletableFuture.supplyAsync(() -> {
      Map<Urn, String> urnsToIdsMap;
      Set<Urn> urns;
      Map<Urn, EntityResponse> entityResponseMap;

      try {
        urnsToIdsMap = buildUrnToIdMap(input.getIds(), authentication);
        urns = urnsToIdsMap.keySet();
        entityResponseMap = _entityClient.batchGetV2(DATAHUB_STEP_STATE_ENTITY_NAME, urns,
            ImmutableSet.of(DATAHUB_STEP_STATE_PROPERTIES_ASPECT_NAME), authentication);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      final Map<Urn, DataHubStepStateProperties> stepStatePropertiesMap = new HashMap<>();
      for (Map.Entry<Urn, EntityResponse> entry : entityResponseMap.entrySet()) {
        final Urn urn = entry.getKey();
        final DataHubStepStateProperties stepStateProperties = getStepStateProperties(urn, entry.getValue());
        if (stepStateProperties != null) {
          stepStatePropertiesMap.put(urn, stepStateProperties);
        }
      }

      final List<StepStateResult> results = stepStatePropertiesMap.entrySet()
          .stream()
          .map(entry -> buildStepStateResult(urnsToIdsMap.get(entry.getKey()), entry.getValue()))
          .collect(Collectors.toList());
      final BatchGetStepStatesResult result = new BatchGetStepStatesResult();
      result.setResults(results);
      return result;
    });
  }

  @Nonnull
  private Map<Urn, String> buildUrnToIdMap(@Nonnull final List<String> ids, @Nonnull final Authentication authentication)
      throws RemoteInvocationException {
    final Map<Urn, String> urnToIdMap = new HashMap<>();
    for (final String id : ids) {
      final Urn urn = getStepStateUrn(id);
      if (_entityClient.exists(urn, authentication)) {
        urnToIdMap.put(urn, id);
      }
    }

    return urnToIdMap;
  }

  @Nonnull
  private Urn getStepStateUrn(@Nonnull final String id) {
    final DataHubStepStateKey stepStateKey = new DataHubStepStateKey().setId(id);
    return convertEntityKeyToUrn(stepStateKey, DATAHUB_STEP_STATE_ENTITY_NAME);
  }

  @Nullable
  private DataHubStepStateProperties getStepStateProperties(@Nonnull final Urn urn,
      @Nonnull final EntityResponse entityResponse) {
    final EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    // If aspect is not present, log the error and return null.
    if (!aspectMap.containsKey(DATAHUB_STEP_STATE_PROPERTIES_ASPECT_NAME)) {
      log.error("Failed to find step state properties for urn: " + urn);
      return null;
    }
    return new DataHubStepStateProperties(aspectMap.get(DATAHUB_STEP_STATE_PROPERTIES_ASPECT_NAME).getValue().data());
  }

  @Nonnull
  private StepStateResult buildStepStateResult(@Nonnull final String id,
      @Nonnull final DataHubStepStateProperties stepStateProperties) {
    final StepStateResult result = new StepStateResult();
    result.setId(id);
    final List<StringMapEntry> mappedProperties = stepStateProperties
        .getProperties()
        .entrySet()
        .stream()
        .map(entry -> buildStringMapEntry(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
    result.setProperties(mappedProperties);
    return result;
  }

  @Nonnull
  private StringMapEntry buildStringMapEntry(@Nonnull final String key, @Nonnull final String value) {
    final StringMapEntry entry = new StringMapEntry();
    entry.setKey(key);
    entry.setValue(value);
    return entry;
  }
}