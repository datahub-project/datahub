package com.linkedin.datahub.graphql.types.view;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.services.RestrictedService;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataHubViewType
    implements com.linkedin.datahub.graphql.types.EntityType<DataHubView, String> {
  public static final Set<String> ASPECTS_TO_FETCH = ImmutableSet.of(DATAHUB_VIEW_INFO_ASPECT_NAME);
  private final EntityClient _entityClient;
  @Nullable private final RestrictedService _restrictedService;

  public DataHubViewType(final EntityClient entityClient) {
    this(entityClient, null);
  }

  public DataHubViewType(
      final EntityClient entityClient, @Nullable final RestrictedService restrictedService) {
    _entityClient = entityClient;
    _restrictedService = restrictedService;
  }

  @Override
  public EntityType type() {
    return EntityType.DATAHUB_VIEW;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<DataHubView> objectClass() {
    return DataHubView.class;
  }

  @Override
  public RestrictedService getRestrictedService() {
    return _restrictedService;
  }

  @Override
  public List<DataFetcherResult<DataHubView>> batchLoadWithoutAuthorization(
      @Nonnull List<String> urnStrs, @Nonnull QueryContext context) throws Exception {
    try {
      final Set<Urn> urns = urnStrs.stream().map(this::getUrn).collect(Collectors.toSet());

      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(), DATAHUB_VIEW_ENTITY_NAME, urns, ASPECTS_TO_FETCH);

      return mapResponsesToBatchResults(urnStrs, entities, DataHubViewMapper::map, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Views", e);
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
