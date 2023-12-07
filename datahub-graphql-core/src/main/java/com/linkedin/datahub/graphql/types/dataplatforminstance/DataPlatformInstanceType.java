package com.linkedin.datahub.graphql.types.dataplatforminstance;

import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.DataPlatformInstance;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.dataplatforminstance.mappers.DataPlatformInstanceMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import graphql.execution.DataFetcherResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;

public class DataPlatformInstanceType
    implements SearchableEntityType<DataPlatformInstance, String>,
        com.linkedin.datahub.graphql.types.EntityType<DataPlatformInstance, String> {

  static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          Constants.DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME,
          Constants.DATA_PLATFORM_INSTANCE_PROPERTIES_ASPECT_NAME,
          Constants.DEPRECATION_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          Constants.STATUS_ASPECT_NAME);
  private final EntityClient _entityClient;

  public DataPlatformInstanceType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.DATA_PLATFORM_INSTANCE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<DataPlatformInstance> objectClass() {
    return DataPlatformInstance.class;
  }

  @Override
  public List<DataFetcherResult<DataPlatformInstance>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> dataPlatformInstanceUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME,
              new HashSet<>(dataPlatformInstanceUrns),
              ASPECTS_TO_FETCH,
              context.getAuthentication());

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : dataPlatformInstanceUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<DataPlatformInstance>newResult()
                          .data(DataPlatformInstanceMapper.map(gmsResult))
                          .build())
          .collect(Collectors.toList());

    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load DataPlatformInstance", e);
    }
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull final QueryContext context)
      throws Exception {
    throw new NotImplementedException(
        "Searchable type (deprecated) not implemented on DataPlatformInstance entity type");
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter filters,
      int limit,
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            DATA_PLATFORM_INSTANCE_ENTITY_NAME, query, filters, limit, context.getAuthentication());
    return AutoCompleteResultsMapper.map(result);
  }
}
