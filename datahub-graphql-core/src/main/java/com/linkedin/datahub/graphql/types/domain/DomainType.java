package com.linkedin.datahub.graphql.types.domain;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
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
import javax.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;

public class DomainType
    implements SearchableEntityType<Domain, String>,
        com.linkedin.datahub.graphql.types.EntityType<Domain, String> {

  static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          Constants.DOMAIN_KEY_ASPECT_NAME,
          Constants.DOMAIN_PROPERTIES_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
          Constants.STRUCTURED_PROPERTIES_ASPECT_NAME,
          Constants.FORMS_ASPECT_NAME);
  private final EntityClient _entityClient;

  public DomainType(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public EntityType type() {
    return EntityType.DOMAIN;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Domain> objectClass() {
    return Domain.class;
  }

  @Override
  public List<DataFetcherResult<Domain>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> domainUrns = urns.stream().map(this::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.DOMAIN_ENTITY_NAME,
              new HashSet<>(domainUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>(urns.size());
      for (Urn urn : domainUrns) {
        gmsResults.add(entities.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<Domain>newResult()
                          .data(DomainMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Domains", e);
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
        "Searchable type (deprecated) not implemented on Domain entity type");
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
            context.getOperationContext(), Constants.DOMAIN_ENTITY_NAME, query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  private Urn getUrn(final String urnStr) {
    try {
      return Urn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(String.format("Failed to convert urn string %s into Urn", urnStr));
    }
  }
}
