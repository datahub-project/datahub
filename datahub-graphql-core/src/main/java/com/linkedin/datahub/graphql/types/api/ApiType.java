package com.linkedin.datahub.graphql.types.api;

import static com.linkedin.metadata.Constants.API_ENTITY_NAME;
import static com.linkedin.metadata.Constants.API_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.API_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.API_SIGNATURE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOSSARY_TERMS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.REST_API_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SUB_TYPES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Api;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.api.mappers.ApiMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.util.AspectUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
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
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.NotImplementedException;

@RequiredArgsConstructor
public class ApiType
    implements SearchableEntityType<Api, String>,
        com.linkedin.datahub.graphql.types.EntityType<Api, String> {

  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          API_PROPERTIES_ASPECT_NAME,
          API_SIGNATURE_ASPECT_NAME,
          REST_API_PROPERTIES_ASPECT_NAME,
          SUB_TYPES_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME,
          DOMAINS_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          STATUS_ASPECT_NAME,
          STRUCTURED_PROPERTIES_ASPECT_NAME,
          VERSION_PROPERTIES_ASPECT_NAME,
          DATA_PLATFORM_INSTANCE_ASPECT_NAME);

  private final EntityClient _entityClient;

  @Override
  public EntityType type() {
    return EntityType.API;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Api> objectClass() {
    return Api.class;
  }

  @Override
  public List<DataFetcherResult<Api>> batchLoad(
      @Nonnull final List<String> urns, @Nonnull final QueryContext context) throws Exception {
    final List<Urn> apiUrns = urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      // Determine optimal aspects to fetch based on GraphQL field selections
      final Set<String> aspectsToResolve =
          AspectUtils.getOptimizedAspects(context, name(), ASPECTS_TO_FETCH, API_KEY_ASPECT_NAME);
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              API_ENTITY_NAME,
              new HashSet<>(apiUrns),
              aspectsToResolve);

      final List<EntityResponse> gmsResults = new ArrayList<>(urns.size());
      for (Urn urn : apiUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<Api>newResult()
                          .data(ApiMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load APIs", e);
    }
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull final String query,
      @Nullable final String field,
      @Nullable final Filter filters,
      @Nullable final Integer limit,
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            context.getOperationContext(), API_ENTITY_NAME, query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  @Override
  public SearchResults search(
      @Nonnull final String query,
      @Nullable final List<FacetFilterInput> filters,
      final int start,
      @Nullable final Integer count,
      @Nonnull final QueryContext context)
      throws Exception {
    throw new NotImplementedException(
        "Searchable type (deprecated) not implemented on Api entity type");
  }
}
