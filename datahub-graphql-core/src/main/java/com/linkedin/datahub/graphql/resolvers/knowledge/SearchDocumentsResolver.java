package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.SearchDocumentsInput;
import com.linkedin.datahub.graphql.generated.SearchDocumentsResult;
import com.linkedin.datahub.graphql.types.knowledge.DocumentMapper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.DocumentService;
import com.linkedin.metadata.utils.CriterionUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for searching Documents with hybrid semantic search and advanced filtering support.
 *
 * <p>Filtering behavior: - PUBLISHED documents are shown to all users - UNPUBLISHED documents are
 * only shown if owned by the current user or a group they belong to - By default, only PUBLISHED
 * documents are searched unless specific states are requested via input
 */
@Slf4j
@RequiredArgsConstructor
public class SearchDocumentsResolver
    implements DataFetcher<CompletableFuture<SearchDocumentsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "*";

  private final DocumentService _documentService;
  private final EntityClient _entityClient;
  private final GroupService _groupService;

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
                _groupService.getGroupsForUser(context.getOperationContext(), currentUserUrn);
            final List<String> userAndGroupUrns = new ArrayList<>();
            userAndGroupUrns.add(currentUserUrn.toString());
            userGroupUrns.forEach(groupUrn -> userAndGroupUrns.add(groupUrn.toString()));

            // Build filter that combines user filters with ownership constraints
            // Filter logic: (PUBLISHED) OR (UNPUBLISHED AND owned-by-user-or-groups)
            List<Criterion> baseUserCriteria = buildBaseUserCriteria(input);
            Filter filter =
                DocumentSearchFilterUtils.buildCombinedFilter(baseUserCriteria, userAndGroupUrns);

            // Step 1: Search using service to get URNs
            final SearchResult gmsResult;
            try {
              gmsResult =
                  _documentService.searchDocuments(
                      context.getOperationContext(), query, filter, null, start, count);
            } catch (Exception e) {
              throw new RuntimeException("Failed to search documents", e);
            }

            // Step 2: Extract URNs from search results
            final List<Urn> documentUrns =
                gmsResult.getEntities().stream()
                    .map(SearchEntity::getEntity)
                    .collect(Collectors.toList());

            // Step 3: Batch hydrate/resolve the Document entities
            final Map<Urn, EntityResponse> entities =
                _entityClient.batchGetV2(
                    context.getOperationContext(),
                    Constants.DOCUMENT_ENTITY_NAME,
                    new HashSet<>(documentUrns),
                    com.linkedin.datahub.graphql.types.knowledge.DocumentType.ASPECTS_TO_FETCH);

            // Step 4: Map entities in the same order as search results
            final List<EntityResponse> orderedEntityResponses = new ArrayList<>();
            for (Urn urn : documentUrns) {
              orderedEntityResponses.add(entities.getOrDefault(urn, null));
            }

            // Step 5: Convert to GraphQL Document objects
            final List<Document> documents =
                orderedEntityResponses.stream()
                    .filter(entityResponse -> entityResponse != null)
                    .map(entityResponse -> DocumentMapper.map(context, entityResponse))
                    .collect(Collectors.toList());

            // Step 6: Build the result
            final SearchDocumentsResult result = new SearchDocumentsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setDocuments(documents);

            // Map facets
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

  /**
   * Builds the base user criteria from the search input (excludes state filtering). These criteria
   * are common to both published and unpublished document searches.
   */
  private List<Criterion> buildBaseUserCriteria(SearchDocumentsInput input) {
    List<Criterion> criteria = new ArrayList<>();

    // Add parent documents filter if provided
    if (input.getParentDocuments() != null && !input.getParentDocuments().isEmpty()) {
      criteria.add(
          CriterionUtils.buildCriterion(
              "parentDocument", Condition.EQUAL, input.getParentDocuments()));
    } else if (input.getRootOnly() != null && input.getRootOnly()) {
      // Filter for root-level documents only (no parent)
      Criterion noParentCriterion = new Criterion();
      noParentCriterion.setField("parentDocument");
      noParentCriterion.setCondition(Condition.IS_NULL);
      criteria.add(noParentCriterion);
    }

    // Add types filter if provided (now using subTypes aspect)
    if (input.getTypes() != null && !input.getTypes().isEmpty()) {
      criteria.add(CriterionUtils.buildCriterion("subTypes", Condition.EQUAL, input.getTypes()));
    }

    // Add domains filter if provided
    if (input.getDomains() != null && !input.getDomains().isEmpty()) {
      criteria.add(CriterionUtils.buildCriterion("domains", Condition.EQUAL, input.getDomains()));
    }

    // Add relatedAssets filter if provided
    if (input.getRelatedAssets() != null && !input.getRelatedAssets().isEmpty()) {
      criteria.add(
          CriterionUtils.buildCriterion(
              "relatedAssets", Condition.EQUAL, input.getRelatedAssets()));
    }

    // Add source type filter if provided (if null, search all)
    if (input.getSourceType() != null) {
      criteria.add(
          CriterionUtils.buildCriterion(
              "sourceType",
              Condition.EQUAL,
              Collections.singletonList(input.getSourceType().toString())));
    }

    return criteria;
  }
}
