package com.linkedin.datahub.graphql.types.restricted;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RestrictedType implements EntityType<Restricted, String> {
  public static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of();

  private final EntityClient _entityClient;
  private final RestrictedService _restrictedService;

  @Override
  public com.linkedin.datahub.graphql.generated.EntityType type() {
    return com.linkedin.datahub.graphql.generated.EntityType.RESTRICTED;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Restricted> objectClass() {
    return Restricted.class;
  }

  @Override
  public List<DataFetcherResult<Restricted>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> restrictedUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());
    final List<Urn> entityUrns =
        restrictedUrns.stream()
            .map(_restrictedService::decryptRestrictedUrn)
            .collect(Collectors.toList());

    // Create a map for entityType: entityUrns so we can fetch by entity type below
    final Map<String, List<Urn>> entityTypeToUrns = createEntityTypeToUrnsMap(entityUrns);

    try {
      // Fetch from the DB for each entity type and add to one result map
      final Map<Urn, EntityResponse> entities = new HashMap<>();
      entityTypeToUrns
          .keySet()
          .forEach(
              entityType -> {
                try {
                  entities.putAll(
                      _entityClient.batchGetV2(
                          context.getOperationContext(),
                          entityType,
                          new HashSet<>(entityTypeToUrns.get(entityType)),
                          ASPECTS_TO_FETCH));
                } catch (Exception e) {
                  throw new RuntimeException("Failed to fetch restricted entities", e);
                }
              });

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : entityUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<Restricted>newResult()
                          .data(RestrictedMapper.map(gmsResult, _restrictedService))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Queries", e);
    }
  }

  private Map<String, List<Urn>> createEntityTypeToUrnsMap(final List<Urn> urns) {
    final Map<String, List<Urn>> entityTypeToUrns = new HashMap<>();
    urns.forEach(
        urn -> {
          String entityType = urn.getEntityType();
          List<Urn> existingUrns =
              entityTypeToUrns.computeIfAbsent(entityType, k -> new ArrayList<>());
          existingUrns.add(urn);
        });
    return entityTypeToUrns;
  }
}
