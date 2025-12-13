package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.datahub.authentication.group.GroupService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Document;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.RelatedDocumentsInput;
import com.linkedin.datahub.graphql.generated.RelatedDocumentsResult;
import com.linkedin.datahub.graphql.types.knowledge.DocumentMapper;
import com.linkedin.datahub.graphql.types.mappers.MapperUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
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
 * Resolver for fetching context documents related to an entity. Returns documents where the
 * entity's URN appears in the relatedAssets field.
 *
 * <p>Filtering behavior: - PUBLISHED documents are shown to all users - UNPUBLISHED documents are
 * only shown if owned by the current user or a group they belong to
 */
@Slf4j
@RequiredArgsConstructor
public class RelatedDocumentsResolver
    implements DataFetcher<CompletableFuture<RelatedDocumentsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 100; // Limit to 100 most recently updated documents
  private static final String DEFAULT_QUERY = "*";

  private final DocumentService _documentService;
  private final EntityClient _entityClient;
  private final GroupService _groupService;

  @Override
  public CompletableFuture<RelatedDocumentsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          log.debug("RelatedDocumentsResolver.supplyAsync lambda executing");
          try {
            // Get the parent entity from the source
            final Entity parentEntity = (Entity) environment.getSource();
            if (parentEntity == null || parentEntity.getUrn() == null) {
              log.error("Parent entity is null or missing URN");
              throw new RuntimeException("Parent entity URN is required");
            }

            final String parentEntityUrn = parentEntity.getUrn();
            log.debug("Fetching related documents for entity: {}", parentEntityUrn);

            final RelatedDocumentsInput input =
                bindArgument(environment.getArgument("input"), RelatedDocumentsInput.class);
            final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
            final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
            // Get current user and their groups for ownership filtering
            final Urn currentUserUrn = Urn.createFromString(context.getActorUrn());
            final List<Urn> userGroupUrns =
                _groupService.getGroupsForUser(context.getOperationContext(), currentUserUrn);
            final List<String> userAndGroupUrns = new ArrayList<>();
            userAndGroupUrns.add(currentUserUrn.toString());
            userGroupUrns.forEach(groupUrn -> userAndGroupUrns.add(groupUrn.toString()));

            // Build base user criteria from input
            List<Criterion> baseUserCriteria = buildBaseUserCriteria(input, parentEntityUrn);

            // Build filter that combines user filters with ownership constraints
            // Filter logic: (PUBLISHED) OR (UNPUBLISHED AND owned-by-user-or-groups)
            // Note: applyShowInGlobalContext=false because related documents should show
            // all context documents (those are meant to be discovered through their related
            // entities)
            Filter filter =
                DocumentSearchFilterUtils.buildCombinedFilter(
                    baseUserCriteria, userAndGroupUrns, false);

            // Step 1: Search using service to get URNs
            // Sort by lastModifiedAt descending to show most recently updated documents first
            final SortCriterion sortCriterion =
                new SortCriterion().setField("lastModifiedAt").setOrder(SortOrder.DESCENDING);
            final SearchResult gmsResult;
            try {
              gmsResult =
                  _documentService.searchDocuments(
                      context.getOperationContext(),
                      DEFAULT_QUERY,
                      filter,
                      sortCriterion,
                      start,
                      count);
            } catch (Exception e) {
              throw new RuntimeException("Failed to search context documents", e);
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
            final RelatedDocumentsResult result = new RelatedDocumentsResult();
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
            String entityUrn = null;
            try {
              final Entity parentEntity = (Entity) environment.getSource();
              entityUrn = parentEntity != null ? parentEntity.getUrn() : "unknown";
            } catch (Exception ignored) {
              // Ignore errors getting entity URN for logging
            }
            log.error(
                "Failed to fetch related documents for entity {}: {}",
                entityUrn,
                e.getMessage(),
                e);
            throw new RuntimeException("Failed to fetch related documents", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  /**
   * Builds the base user criteria from the input (excludes state filtering). These criteria are
   * common to both published and unpublished document searches. Automatically adds the
   * relatedAssets filter with the parent entity's URN.
   *
   * @param input The context documents input
   * @param parentEntityUrn The URN of the parent entity (automatically added to relatedAssets
   *     filter)
   * @return List of criteria
   */
  private List<Criterion> buildBaseUserCriteria(
      RelatedDocumentsInput input, String parentEntityUrn) {
    List<Criterion> criteria = new ArrayList<>();

    // Automatically add relatedAssets filter with parent entity URN
    criteria.add(
        CriterionUtils.buildCriterion(
            "relatedAssets", Condition.EQUAL, Collections.singletonList(parentEntityUrn)));

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

    // NOTE: State filtering is handled in DocumentSearchFilterUtils.buildCombinedFilter with
    // ownership logic
    // Do not add state filters here

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
