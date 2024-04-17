package com.linkedin.datahub.graphql.types.businessattribute;

import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.BUSINESS_ATTRIBUTE_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.businessattribute.mappers.BusinessAttributeMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchResult;
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

@RequiredArgsConstructor
public class BusinessAttributeType implements SearchableEntityType<BusinessAttribute, String> {

  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME,
          BUSINESS_ATTRIBUTE_KEY_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          STATUS_ASPECT_NAME);
  private static final Set<String> FACET_FIELDS = ImmutableSet.of("");
  private final EntityClient _entityClient;

  @Override
  public EntityType type() {
    return EntityType.BUSINESS_ATTRIBUTE;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<BusinessAttribute> objectClass() {
    return BusinessAttribute.class;
  }

  @Override
  public List<DataFetcherResult<BusinessAttribute>> batchLoad(
      @Nonnull List<String> urns, @Nonnull QueryContext context) throws Exception {
    final List<Urn> businessAttributeUrns =
        urns.stream().map(UrnUtils::getUrn).collect(Collectors.toList());

    try {
      final Map<Urn, EntityResponse> businessAttributeMap =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              BUSINESS_ATTRIBUTE_ENTITY_NAME,
              new HashSet<>(businessAttributeUrns),
              ASPECTS_TO_FETCH);

      final List<EntityResponse> gmsResults = new ArrayList<>();
      for (Urn urn : businessAttributeUrns) {
        gmsResults.add(businessAttributeMap.getOrDefault(urn, null));
      }
      return gmsResults.stream()
          .map(
              gmsResult ->
                  gmsResult == null
                      ? null
                      : DataFetcherResult.<BusinessAttribute>newResult()
                          .data(BusinessAttributeMapper.map(context, gmsResult))
                          .build())
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Business Attributes", e);
    }
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      int count,
      @Nonnull QueryContext context)
      throws Exception {
    final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
    final SearchResult searchResult =
        _entityClient.search(
            context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
            "businessAttribute",
            query,
            facetFilters,
            start,
            count);
    return UrnSearchResultsMapper.map(context, searchResult);
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter filters,
      int limit,
      @Nonnull QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            context.getOperationContext(), "businessAttribute", query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }
}
