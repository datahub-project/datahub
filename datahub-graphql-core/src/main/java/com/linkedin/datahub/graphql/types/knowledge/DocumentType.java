package com.linkedin.datahub.graphql.types.knowledge;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.Document;
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
import io.datahubproject.metadata.services.RestrictedService;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.NotImplementedException;

/** GraphQL Type implementation for Document entity. Supports batch loading and autocomplete. */
public class DocumentType
    implements SearchableEntityType<Document, String>,
        com.linkedin.datahub.graphql.types.EntityType<Document, String> {

  public static final Set<String> ASPECTS_TO_FETCH =
      ImmutableSet.of(
          Constants.DOCUMENT_KEY_ASPECT_NAME,
          Constants.DOCUMENT_INFO_ASPECT_NAME,
          Constants.DOCUMENT_SETTINGS_ASPECT_NAME,
          Constants.OWNERSHIP_ASPECT_NAME,
          Constants.STATUS_ASPECT_NAME,
          Constants.BROWSE_PATHS_V2_ASPECT_NAME,
          Constants.STRUCTURED_PROPERTIES_ASPECT_NAME,
          Constants.DOMAINS_ASPECT_NAME,
          Constants.SUB_TYPES_ASPECT_NAME,
          Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
          Constants.GLOBAL_TAGS_ASPECT_NAME,
          Constants.GLOSSARY_TERMS_ASPECT_NAME,
          Constants.INSTITUTIONAL_MEMORY_ASPECT_NAME,
          Constants.DOCUMENTATION_ASPECT_NAME);

  private final EntityClient _entityClient;
  @Nullable private final RestrictedService _restrictedService;

  public DocumentType(final EntityClient entityClient) {
    this(entityClient, null);
  }

  public DocumentType(
      final EntityClient entityClient, @Nullable final RestrictedService restrictedService) {
    _entityClient = entityClient;
    _restrictedService = restrictedService;
  }

  @Override
  public EntityType type() {
    return EntityType.DOCUMENT;
  }

  @Override
  public Function<Entity, String> getKeyProvider() {
    return Entity::getUrn;
  }

  @Override
  public Class<Document> objectClass() {
    return Document.class;
  }

  @Override
  public RestrictedService getRestrictedService() {
    return _restrictedService;
  }

  @Override
  public List<DataFetcherResult<Document>> batchLoadWithoutAuthorization(
      @Nonnull List<String> urnStrs, @Nonnull QueryContext context) throws Exception {
    try {
      final Set<Urn> urns = urnStrs.stream().map(this::getUrn).collect(Collectors.toSet());

      final Map<Urn, EntityResponse> entities =
          _entityClient.batchGetV2(
              context.getOperationContext(),
              Constants.DOCUMENT_ENTITY_NAME,
              urns,
              ASPECTS_TO_FETCH);

      return mapResponsesToBatchResults(urnStrs, entities, DocumentMapper::map, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to batch load Documents", e);
    }
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
        "Searchable type (deprecated) not implemented on Document entity type. Use searchDocuments query instead.");
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
            context.getOperationContext(), Constants.DOCUMENT_ENTITY_NAME, query, filters, limit);
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
