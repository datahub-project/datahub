package com.linkedin.datahub.graphql.resolvers.assertion;

import static com.linkedin.metadata.Constants.ASSERTION_ENTITY_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.Assertion;
import com.linkedin.datahub.graphql.generated.AssertionsQueryFilterInput;
import com.linkedin.datahub.graphql.generated.ListAssertionsResult;
import com.linkedin.datahub.graphql.generated.LogicalOperator;
import com.linkedin.datahub.graphql.generated.SearchSortInput;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.SearchUtils;
import com.linkedin.datahub.graphql.types.assertion.AssertionMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class ListAssertionsResolver
    implements DataFetcher<CompletableFuture<ListAssertionsResult>> {
  private final EntityClient entityClient;
  private final GraphClient graphClient;

  public ListAssertionsResolver(
      @Nonnull final EntityClient entityClient, @Nonnull final GraphClient graphClient) {
    this.entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    this.graphClient = Objects.requireNonNull(graphClient, "graphClient must not be null");
  }

  @Override
  public CompletableFuture<ListAssertionsResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String query = environment.getArgument("query");
    final int start = environment.getArgumentOrDefault("start", 0);
    final int count = environment.getArgumentOrDefault("count", 10);
    final AssertionsQueryFilterInput filter =
        ResolverUtils.bindArgument(
            environment.getArgument("filter"), AssertionsQueryFilterInput.class);
    final SearchSortInput sort =
        ResolverUtils.bindArgument(environment.getArgument("sort"), SearchSortInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return listAssertions(context, query, start, count, filter, sort);
          } catch (final Exception e) {
            throw new DataHubGraphQLException(
                String.format("Failed to list assertions: %s", e.getMessage()),
                DataHubGraphQLErrorCode.SERVER_ERROR);
          }
        });
  }

  private ListAssertionsResult listAssertions(
      final QueryContext context,
      final String query,
      final int start,
      final int count,
      final AssertionsQueryFilterInput filterInput,
      final SearchSortInput sortInput)
      throws RemoteInvocationException, URISyntaxException {
    final OperationContext opContext = context.getOperationContext();

    // 1. Search for the assertion urns that match filters
    final SearchResult searchResult =
        entityClient.search(
            opContext,
            ASSERTION_ENTITY_NAME,
            query,
            inputToFilter(filterInput),
            sortInput != null
                ? sortInput.getSortCriteria().stream()
                    .map(SearchUtils::mapSortCriterion)
                    .collect(Collectors.toList())
                : null,
            start,
            count);

    if (searchResult == null || searchResult.getNumEntities() == 0) {
      return ListAssertionsResult.builder()
          .setStart(searchResult.getFrom())
          .setCount(searchResult.getPageSize())
          .setTotal(searchResult.getNumEntities())
          .setAssertions(new ArrayList<>())
          .build();
    }

    final List<Urn> assertionUrns =
        searchResult.getEntities().stream().map(SearchEntity::getEntity).toList();

    // 2. Hydrate the results with the assertion entities
    final List<Assertion> orderedAssertions = hydrateAssertions(context, assertionUrns);

    // 3. Return the result
    return ListAssertionsResult.builder()
        .setStart(searchResult.getFrom())
        .setCount(searchResult.getPageSize())
        .setTotal(searchResult.getNumEntities())
        .setAssertions(orderedAssertions)
        .build();
  }

  final List<Assertion> hydrateAssertions(
      @Nonnull final QueryContext context, @Nonnull List<Urn> assertionUrns)
      throws URISyntaxException, RemoteInvocationException {
    // 1. Fetch the assertion entities
    final Map<Urn, EntityResponse> entities =
        entityClient.batchGetV2(
            context.getOperationContext(),
            Constants.ASSERTION_ENTITY_NAME,
            new HashSet<>(assertionUrns),
            null,
            false);

    // 2. Map GMS assertion model to GraphQL model
    final List<EntityResponse> gmsResults = new ArrayList<>();
    for (Urn urn : assertionUrns) {
      gmsResults.add(entities.getOrDefault(urn, null));
    }
    final Map<String, Assertion> assertionsMap =
        gmsResults.stream()
            .filter(Objects::nonNull)
            .map(r -> AssertionMapper.map(context, r))
            .collect(Collectors.toMap(Assertion::getUrn, assertion -> assertion));

    // 3. Sort the assertions based on the order of the urns in the input
    return assertionUrns.stream()
        .map(urn -> assertionsMap.get(urn.toString()))
        .filter(Objects::nonNull)
        .toList();
  }

  /**
   * Converts the AssertionsQueryFilterInput to a Filter object. NOTE: The time range filters
   * require special handling because they can be ANDed or ORed. In the case that they are ORed, we
   * need to create a disjunction with the rest of the conjunctive criteria. I.e., $or: [{ $and: [ {
   * assertionLastFailed: ...}, ...nonTimeRangeCriteria ] }, { $and: [ { assertionLastPassed: ...},
   * ...nonTimeRangeCriteria ] }]
   */
  @VisibleForTesting
  protected Filter inputToFilter(final AssertionsQueryFilterInput filterInput) {
    if (filterInput == null) {
      return null;
    }

    final List<ConjunctiveCriterion> orList = new ArrayList<>();
    // 1. If there are no time range filters, we can just construct the non-time range filters
    if (filterInput.getTimeRanges() == null) {
      final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
      conjunctiveCriterion.setAnd(buildNonTimeRangeCriterion(filterInput));
      orList.add(conjunctiveCriterion);
    }
    // 2. IF we're ANDing the time ranges, we can just add them directly into a single criterion
    // array
    else if (filterInput.getTimeRanges().getOperator().equals(LogicalOperator.AND)) {
      // 2.1 Get the non-time range filters
      final CriterionArray criterionArray = buildNonTimeRangeCriterion(filterInput);
      // 2.2 Add all the time range filters to the criterion array
      filterInput
          .getTimeRanges()
          .getFilter()
          .forEach(
              filter -> {
                criterionArray.add(
                    new Criterion()
                        .setField(filter.getField())
                        .setCondition(Condition.valueOf(filter.getCondition().name()))
                        .setValues(new StringArray(filter.getValues())));
              });
      // 2.3 Add to the conjunctive criterion list
      final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
      conjunctiveCriterion.setAnd(criterionArray);
      orList.add(conjunctiveCriterion);
    }
    // 3. IF we're ORing the time ranges...
    else {
      // ...then, for each time range...
      filterInput
          .getTimeRanges()
          .getFilter()
          .forEach(
              filter -> {
                // 3.1 Create a criterion array with the non-time range filters
                final CriterionArray criterionArray = buildNonTimeRangeCriterion(filterInput);

                // 3.2 Add this time range to the criterion array
                criterionArray.add(
                    new Criterion()
                        .setField(filter.getField())
                        .setCondition(Condition.valueOf(filter.getCondition().name()))
                        .setValues(new StringArray(filter.getValues())));

                // 3.3 Create a conjunctive criterion for this time range, add it to the overall
                // disjunction list
                final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
                conjunctiveCriterion.setAnd(criterionArray);
                orList.add(conjunctiveCriterion);
              });
    }

    return new Filter().setOr(new ConjunctiveCriterionArray(orList));
  }

  /**
   * Builds a criterion array for the non-time range related filters in the
   * AssertionsQueryFilterInput.
   */
  private CriterionArray buildNonTimeRangeCriterion(
      @Nonnull final AssertionsQueryFilterInput filterInput) {
    final CriterionArray criterionArray = new CriterionArray();

    if (filterInput.getCreator() != null && !filterInput.getCreator().isEmpty()) {
      criterionArray.add(
          new Criterion().setField("creator").setValues(new StringArray(filterInput.getCreator())));
    }
    if (filterInput.getTags() != null && !filterInput.getTags().isEmpty()) {
      criterionArray.add(
          new Criterion().setField("tags").setValues(new StringArray(filterInput.getTags())));
    }
    if (filterInput.getType() != null && !filterInput.getType().isEmpty()) {
      criterionArray.add(
          new Criterion()
              .setField("type")
              .setValues(
                  new StringArray(
                      filterInput.getType().stream().map(Enum::name).collect(Collectors.toSet()))));
    }
    if (filterInput.getAsserteeDomain() != null && !filterInput.getAsserteeDomain().isEmpty()) {
      criterionArray.add(
          new Criterion()
              .setField("asserteeDomains")
              .setValues(new StringArray(filterInput.getAsserteeDomain())));
    }
    if (filterInput.getAsserteeDataProduct() != null
        && !filterInput.getAsserteeDataProduct().isEmpty()) {
      criterionArray.add(
          new Criterion()
              .setField("asserteeDataProducts")
              .setValues(new StringArray(filterInput.getAsserteeDataProduct())));
    }
    if (filterInput.getAsserteeTags() != null && !filterInput.getAsserteeTags().isEmpty()) {
      criterionArray.add(
          new Criterion()
              .setField("asserteeTags")
              .setValues(new StringArray(filterInput.getAsserteeTags())));
    }
    if (filterInput.getAsserteeTerms() != null && !filterInput.getAsserteeTerms().isEmpty()) {
      criterionArray.add(
          new Criterion()
              .setField("asserteeGlossaryTerms")
              .setValues(new StringArray(filterInput.getAsserteeTerms())));
    }
    if (filterInput.getAsserteeOwner() != null && !filterInput.getAsserteeOwner().isEmpty()) {
      criterionArray.add(
          new Criterion()
              .setField("asserteeOwners")
              .setValues(new StringArray(filterInput.getAsserteeOwner())));
    }
    if (filterInput.getAsserteePlatform() != null && !filterInput.getAsserteePlatform().isEmpty()) {
      criterionArray.add(
          new Criterion()
              .setField("asserteeDataPlatform")
              .setValues(new StringArray(filterInput.getAsserteePlatform())));
    }
    if (filterInput.getAsserteePlatformInstance() != null
        && !filterInput.getAsserteePlatformInstance().isEmpty()) {
      criterionArray.add(
          new Criterion()
              .setField("asserteeDataPlatformInstance")
              .setValues(new StringArray(filterInput.getAsserteePlatformInstance())));
    }
    return criterionArray;
  }
}
