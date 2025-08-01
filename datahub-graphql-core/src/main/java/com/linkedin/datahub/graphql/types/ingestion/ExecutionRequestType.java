package com.linkedin.datahub.graphql.types.ingestion;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ExecutionRequest;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * ExecutionRequestType provides a way to load {@link ExecutionRequest} objects from their URNs. It
 * leverages the {@link EntityClient} to retrieve the entities from the GMS.
 */
@RequiredArgsConstructor
public class ExecutionRequestType
    implements com.linkedin.datahub.graphql.types.EntityType<ExecutionRequest, String> {
  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME,
          Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME);

  /**
   * Returns the {@link EntityType} associated with the {@link ExecutionRequest} type.
   *
   * @return {@link EntityType#EXECUTION_REQUEST}
   */
  @Override
  public EntityType type() {
    return EntityType.EXECUTION_REQUEST;
  }

  /**
   * Returns a function that extracts the URN from an {@link Entity} object.
   *
   * @return a function that extracts the URN from an {@link Entity} object.
   */
  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  /**
   * Returns the class of the object that this type loads.
   *
   * @return the class of the object that this type loads.
   */
  @Override
  public Class<ExecutionRequest> objectClass() {
    return ExecutionRequest.class;
  }

  /**
   * Loads a batch of {@link ExecutionRequest} objects from their URNs.
   *
   * @param urns a list of URNs to load.
   * @param context the query context.
   * @return a list of {@link DataFetcherResult} objects containing the loaded {@link
   *     ExecutionRequest} objects.
   * @throws Exception if an error occurs while loading the entities.
   */
  @Override
  public List<DataFetcherResult<ExecutionRequest>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> executionRunUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.EXECUTION_REQUEST_ENTITY_NAME,
              new HashSet<>(executionRunUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : executionRunUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<ExecutionRequest>newResult()
                          .data(ExecutionRequestMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Execution requests", e);
    }
  }

  private final EntityClient _entityClient;
}
