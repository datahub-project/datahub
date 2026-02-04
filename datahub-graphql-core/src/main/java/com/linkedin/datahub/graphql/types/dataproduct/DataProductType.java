package com.linkedin.datahub.graphql.types.dataproduct;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.dataproduct.mappers.DataProductMapper;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;

public class DataProductType
    implements SearchableEntityType<DataProduct, String>,
        com.linkedin.datahub.graphql.types.EntityType<DataProduct, String> {
  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          DATA_PRODUCT_KEY_ASPECT_NAME,
          DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
          OWNERSHIP_ASPECT_NAME,
          GLOBAL_TAGS_ASPECT_NAME,
          GLOSSARY_TERMS_ASPECT_NAME,
          DOMAINS_ASPECT_NAME,
          INSTITUTIONAL_MEMORY_ASPECT_NAME,
          STRUCTURED_PROPERTIES_ASPECT_NAME,
          FORMS_ASPECT_NAME,
          APPLICATION_MEMBERSHIP_ASPECT_NAME,
          ASSET_SETTINGS_ASPECT_NAME,
          SHARE_ASPECT_NAME,
          ORIGIN_ASPECT_NAME);
  private final EntityClient _entityClient;
  @Nullable private final RestrictedService _restrictedService;

  public DataProductType(final EntityClient entityClient) {
    this(entityClient, null);
  }

  public DataProductType(
      final EntityClient entityClient, @Nullable final RestrictedService restrictedService) {
    _entityClient = entityClient;
    _restrictedService = restrictedService;
  }

  @Override
  public EntityType type() {
    return EntityType.DATA_PRODUCT;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<DataProduct> objectClass() {
    return DataProduct.class;
  }

  @Override
  public RestrictedService getRestrictedService() {
    return _restrictedService;
  }

  @Override
  public List<DataFetcherResult<DataProduct>> batchLoadWithoutAuthorization(
      @Nonnull List<String> urnStrs, @Nonnull QueryContext context) throws Exception {
    try {
      final Set<Urn> urns = urnStrs.stream().map(UrnUtils::getUrn).collect(Collectors.toSet());

      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(), DATA_PRODUCT_ENTITY_NAME, urns, ASPECTS_TO_FETCH);

      return mapResponsesToBatchResults(urnStrs, entities, DataProductMapper::map, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Data Products", e);
    }
  }

  @Override
  public AutoCompleteResults autoComplete(
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter filters,
      @Nullable Integer limit,
      @Nonnull final QueryContext context)
      throws Exception {
    final AutoCompleteResult result =
        _entityClient.autoComplete(
            context.getOperationContext(), DATA_PRODUCT_ENTITY_NAME, query, filters, limit);
    return AutoCompleteResultsMapper.map(context, result);
  }

  @Override
  public SearchResults search(
      @Nonnull String query,
      @Nullable List<FacetFilterInput> filters,
      int start,
      @Nullable Integer count,
      @Nonnull final QueryContext context)
      throws Exception {
    throw new NotImplementedException(
        "Searchable type (deprecated) not implemented on Data Product entity type");
  }
}
