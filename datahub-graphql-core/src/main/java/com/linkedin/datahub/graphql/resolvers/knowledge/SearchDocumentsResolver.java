package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.getSortCriteria;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.mapInputFlags;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.resolveView;
import static com.linkedin.metadata.utils.elasticsearch.FilterUtils.combineFilters;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.DocumentState;
import com.linkedin.datahub.graphql.generated.SearchDocumentsInput;
import com.linkedin.datahub.graphql.generated.SearchDocumentsResult;
import com.linkedin.datahub.graphql.types.knowledge.DocumentMapper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import com.linkedin.metadata.service.DocumentService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for searching Documents with hybrid semantic search and advanced filtering support.
 *
 * <p>Visibility is enforced via {@link DocumentSearchFilterUtils}: PUBLISHED for everyone;
 * UNPUBLISHED for owners / groups / MANAGE_DOCUMENTS. Optional {@code states} narrows those
 * clauses. Optional facet filters (tags, terms, creators, platforms, …) AND onto the base criteria.
 */
@Slf4j
public class SearchDocumentsResolver
    implements DataFetcher<CompletableFuture<SearchDocumentsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "*";
  private static final String SUB_TYPES_FIELD = "typeNames";
  private static final String CREATOR_FIELD = "creator";
  private static final String CREATED_AT_FIELD = "createdAt";
  private static final String LAST_MODIFIED_AT_FIELD = "lastModifiedAt";

  private final DocumentService _documentService;
  private final EntityClient _entityClient;
  private final ViewService _viewService;
  @Nullable private final EntityIndexConfiguration _entityIndexConfiguration;

  public SearchDocumentsResolver(
      @Nonnull final DocumentService documentService,
      @Nonnull final EntityClient entityClient,
      @Nonnull final ViewService viewService) {
    this(documentService, entityClient, viewService, null);
  }

  public SearchDocumentsResolver(
      @Nonnull final DocumentService documentService,
      @Nonnull final EntityClient entityClient,
      @Nonnull final ViewService viewService,
      @Nullable final EntityIndexConfiguration entityIndexConfiguration) {
    _documentService = documentService;
    _entityClient = entityClient;
    _viewService = viewService;
    _entityIndexConfiguration = entityIndexConfiguration;
  }

  @Override
  public CompletableFuture<SearchDocumentsResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final SearchDocumentsInput input =
              bindArgument(environment.getArgument("input"), SearchDocumentsInput.class);
          final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
          final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
          final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();

          try {
            // Get current user and their groups for ownership filtering
            final Urn currentUserUrn = Urn.createFromString(context.getActorUrn());
            final List<Urn> userGroupUrns =
                new ArrayList<>(
                    context.getOperationContext().getSessionActorContext().getGroupMembership());
            final List<String> userAndGroupUrns = new ArrayList<>();
            userAndGroupUrns.add(currentUserUrn.toString());
            userGroupUrns.forEach(groupUrn -> userAndGroupUrns.add(groupUrn.toString()));

            List<Criterion> baseUserCriteria =
                buildBaseUserCriteria(input, context.getOperationContext());
            final boolean canManageDocuments = AuthorizationUtils.canManageDocuments(context);
            final List<String> requestedStates = mapRequestedStates(input.getStates());
            Filter filter =
                DocumentSearchFilterUtils.buildCombinedFilter(
                    baseUserCriteria, userAndGroupUrns, true, canManageDocuments, requestedStates);

            if (input.getViewUrn() != null) {
              final DataHubViewInfo resolvedView =
                  resolveView(
                      context.getOperationContext(),
                      _viewService,
                      UrnUtils.getUrn(input.getViewUrn()));
              if (resolvedView != null) {
                filter = combineFilters(filter, resolvedView.getDefinition().getFilter());
              }
            }

            final List<SortCriterion> sortCriteria = getSortCriteria(input.getSortInput());
            final SortCriterion sortCriterion =
                sortCriteria.isEmpty()
                    ? null
                    : mapAuditSortField(context.getOperationContext(), sortCriteria.get(0));

            // Mirror AggregateAcrossEntitiesResolver: optional facets + searchFlags (maxAggValues).
            // Flags are applied inside DocumentService so GraphQL unit tests that mock the
            // service never need a fully active OperationContext for withSearchFlags.
            final List<String> facets =
                input.getFacets() != null && !input.getFacets().isEmpty()
                    ? input.getFacets()
                    : null;
            final SearchFlags searchFlags =
                input.getSearchFlags() != null
                    ? mapInputFlags(context, input.getSearchFlags())
                    : null;

            final SearchResult gmsResult;
            try {
              gmsResult =
                  _documentService.searchDocuments(
                      context.getOperationContext(),
                      query,
                      filter,
                      sortCriterion,
                      start,
                      count,
                      facets,
                      searchFlags);
            } catch (Exception e) {
              throw new RuntimeException("Failed to search documents", e);
            }

            final List<Urn> documentUrns =
                gmsResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toList());

            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.DOCUMENT_ENTITY_NAME,
                    new HashSet<>(documentUrns),
                    com.linkedin.datahub.graphql.types.knowledge.DocumentType.ASPECTS_TO_FETCH);

            final List<EntityResponse> orderedEntityResponses = new ArrayList<>();
            for (Urn urn : documentUrns) {
              orderedEntityResponses.add(entities.getOrDefault(urn, null));
            }

            final List<Document> documents =
                orderedEntityResponses.stream()
                    .filter(entityResponse -> entityResponse != null)
                    .map(entityResponse -> DocumentMapper.map(context, entityResponse))
                    .collect(Collectors.toList());

            final SearchDocumentsResult result = new SearchDocumentsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setDocuments(documents);

            if (gmsResult.getMetadata() != null
                && gmsResult.getMetadata().getAggregations() != null) {
              result.setFacets(
                  gmsResult.getMetadata().getAggregations().stream()
                      .map(facet -> MapperUtils.mapFacet(context, facet))
                      .collect(Collectors.toList()));
            } else {
              result.setFacets(Collections.emptyList());
            }

            return result;
          } catch (Exception e) {
            log.error("Failed to search documents: {}", e.getMessage());
            throw new RuntimeException("Failed to search documents", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static List<String> mapRequestedStates(List<DocumentState> states) {
    if (states == null || states.isEmpty()) {
      return null;
    }
    return states.stream().map(Enum::name).collect(Collectors.toList());
  }

  /**
   * Builds the base user criteria from the search input (excludes state filtering). These criteria
   * are common to both published and unpublished document searches.
   */
  /**
   * Search V3 stores audit times under {@code _system}, so the V2 sort names are remapped while V3
   * is the keyword read path. Other sort fields pass through unchanged.
   */
  private SortCriterion mapAuditSortField(
      @Nonnull final OperationContext opContext, @Nonnull final SortCriterion sortCriterion) {
    final String v3Field;
    if (CREATED_AT_FIELD.equals(sortCriterion.getField())) {
      v3Field = MappingConstants.SYSTEM_CREATED_TIME_FIELD;
    } else if (LAST_MODIFIED_AT_FIELD.equals(sortCriterion.getField())) {
      v3Field = MappingConstants.SYSTEM_MODIFIED_TIME_FIELD;
    } else {
      return sortCriterion;
    }
    final String indexedField =
        EntitySearchIndexResolver.indexedField(
            _entityIndexConfiguration, opContext, sortCriterion.getField(), v3Field, Intent.SORT);
    return indexedField.equals(sortCriterion.getField())
        ? sortCriterion
        : new SortCriterion().setField(indexedField).setOrder(sortCriterion.getOrder());
  }

  private List<Criterion> buildBaseUserCriteria(
      SearchDocumentsInput input, @Nonnull OperationContext opContext) {
    List<Criterion> criteria = new ArrayList<>();

    if (input.getParentDocuments() != null && !input.getParentDocuments().isEmpty()) {
      criteria.add(
          CriterionUtils.buildCriterion(
              "parentDocument", Condition.EQUAL, input.getParentDocuments()));
    } else if (input.getRootOnly() != null && input.getRootOnly()) {
      Criterion noParentCriterion = new Criterion();
      noParentCriterion.setField("parentDocument");
      noParentCriterion.setCondition(Condition.IS_NULL);
      criteria.add(noParentCriterion);
    }

    if (input.getTypes() != null && !input.getTypes().isEmpty()) {
      // "typeNames" is the indexed ES field. "subTypes" is the aspect name and matches nothing.
      criteria.add(
          CriterionUtils.buildCriterion(SUB_TYPES_FIELD, Condition.EQUAL, input.getTypes()));
    }

    if (input.getDomains() != null && !input.getDomains().isEmpty()) {
      criteria.add(CriterionUtils.buildCriterion("domains", Condition.EQUAL, input.getDomains()));
    }

    if (input.getHasDomain() != null) {
      criteria.add(
          Boolean.TRUE.equals(input.getHasDomain())
              ? CriterionUtils.buildExistsCriterion("domains")
              : CriterionUtils.buildNotExistsCriterion("domains"));
    }

    if (input.getRelatedAssets() != null && !input.getRelatedAssets().isEmpty()) {
      criteria.add(
          CriterionUtils.buildCriterion(
              "relatedAssets", Condition.EQUAL, input.getRelatedAssets()));
    }

    if (input.getSourceType() != null) {
      criteria.add(
          CriterionUtils.buildCriterion(
              "sourceType",
              Condition.EQUAL,
              Collections.singletonList(input.getSourceType().toString())));
    }

    if (input.getTags() != null && !input.getTags().isEmpty()) {
      criteria.add(CriterionUtils.buildCriterion("tags", Condition.EQUAL, input.getTags()));
    }

    if (input.getGlossaryTerms() != null && !input.getGlossaryTerms().isEmpty()) {
      criteria.add(
          CriterionUtils.buildCriterion(
              "glossaryTerms", Condition.EQUAL, input.getGlossaryTerms()));
    }

    if (input.getCreators() != null && !input.getCreators().isEmpty()) {
      criteria.add(
          CriterionUtils.buildCriterion(
              EntitySearchIndexResolver.indexedField(
                  _entityIndexConfiguration,
                  opContext,
                  CREATOR_FIELD,
                  MappingConstants.SYSTEM_CREATED_ACTOR_FIELD,
                  Intent.FILTER),
              Condition.EQUAL,
              input.getCreators()));
    }

    if (input.getPlatforms() != null && !input.getPlatforms().isEmpty()) {
      criteria.add(
          CriterionUtils.buildCriterion("platform", Condition.EQUAL, input.getPlatforms()));
    }

    return criteria;
  }
}
