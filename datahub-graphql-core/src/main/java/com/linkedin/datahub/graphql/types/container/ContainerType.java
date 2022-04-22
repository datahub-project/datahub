package com.linkedin.datahub.graphql.types.container;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.container.mappers.ContainerMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class ContainerType implements com.linkedin.datahub.graphql.types.EntityType<Container, String> {

  static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(
      Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
      Constants.CONTAINER_PROPERTIES_ASPECT_NAME,
      Constants.CONTAINER_EDITABLE_PROPERTIES_ASPECT_NAME,
      Constants.OWNERSHIP_ASPECT_NAME,
      Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
      Constants.STATUS_ASPECT_NAME,
      Constants.SUB_TYPES_ASPECT_NAME,
      Constants.GLOBAL_TAGS_ASPECT_NAME,
      Constants.GLOSSARY_TERMS_ASPECT_NAME,
      Constants.CONTAINER_ASPECT_NAME,
      Constants.DOMAINS_ASPECT_NAME,
      Constants.DEPRECATION_ASPECT_NAME
  );
  private final EntityClient _entityClient;

  public ContainerType(final EntityClient entityClient)  {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.CONTAINER;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Container> objectClass() {
    return Container.class;
  }

  @Override
  public List<DataFetcherResult<Container>> batchLoad(@Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> containerUrns = urns.stream()
        .map(this::getUrn)
        .collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities = _entityClient.batchGetV2(
          Constants.CONTAINER_ENTITY_NAME,
          new HashSet<>(containerUrns),
          ASPECTS_TO_FETCH,
          context.getAuthentication());

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : containerUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(gmsResult ->
              gmsResult == null ? null : DataFetcherResult.<Container>newResult()
                  .data(ContainerMapper.map(gmsResult))
                  .build()
          )
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Container", e);
    }
  }

  private Urn getUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to convert urn string %s into Urn", urnStr));
    }
  }
}