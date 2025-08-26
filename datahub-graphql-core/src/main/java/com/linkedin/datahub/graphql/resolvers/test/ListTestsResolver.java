package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.test.TestUtils.*;
import static com.linkedin.metadata.AcrylConstants.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListTestsInput;
import com.linkedin.datahub.graphql.generated.ListTestsResult;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.*;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.CriterionUtils;
import graphql.VisibleForTesting;
import graphql.com.google.common.collect.ImmutableList;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Resolver used for listing all Tests defined within DataHub. Requires the MANAGE_DOMAINS platform
 * privilege.
 */
public class ListTestsResolver implements DataFetcher<CompletableFuture<ListTestsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final EntityClient _entityClient;

  @VisibleForTesting
  static final SearchFlags SEARCH_FLAGS = new SearchFlags().setFulltext(true).setSkipCache(true);

  public ListTestsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListTestsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (canManageTests(context) || canViewTests(context)) {
            final ListTestsInput input =
                bindArgument(environment.getArgument("input"), ListTestsInput.class);
            final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
            final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
            final String query = input.getQuery() == null ? "" : input.getQuery();
            final Filter filter =
                input.getOrFilters() != null
                    ? ResolverUtils.buildFilter(null, input.getOrFilters())
                    : buildTestsFilter();

            try {
              final SearchResult gmsResult =
                  _entityClient.search(
                      context.getOperationContext().withSearchFlags(flags -> SEARCH_FLAGS),
                      Constants.TEST_ENTITY_NAME,
                      query,
                      filter,
                      Collections.singletonList(
                          new SortCriterion()
                              .setField(TESTS_LAST_UPDATED_TIME_INDEX_FIELD_NAME)
                              .setOrder(SortOrder.DESCENDING)),
                      start,
                      count);

              // Now that we have entities we can bind this to a result.
              final ListTestsResult result = new ListTestsResult();
              result.setStart(gmsResult.getFrom());
              result.setCount(gmsResult.getPageSize());
              result.setTotal(gmsResult.getNumEntities());
              result.setTests(mapUnresolvedTests(gmsResult.getEntities()));
              return result;
            } catch (Exception e) {
              throw new RuntimeException("Failed to list tests", e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }

  // This method maps urns returned from the list endpoint into Partial Test objects which will be
  // resolved be a separate Batch resolver.
  private List<Test> mapUnresolvedTests(final SearchEntityArray entityArray) {
    final List<Test> results = new ArrayList<>();
    for (final SearchEntity entity : entityArray) {
      final Urn urn = entity.getEntity();
      final Test unresolvedTest = new Test();
      unresolvedTest.setUrn(urn.toString());
      unresolvedTest.setType(EntityType.TEST);
      results.add(unresolvedTest);
    }
    return results;
  }

  // Construct a filter which omits any entities that are of the "Forms" source.
  private Filter buildTestsFilter() {
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                ImmutableList.of(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                ImmutableList.of(
                                    // Filter out Forms tests
                                    CriterionUtils.buildCriterion(
                                        "sourceType",
                                        Condition.EQUAL,
                                        true,
                                        "FORMS",
                                        "BULK_FORM_SUBMISSION",
                                        "FORM_PROMPT"),
                                    // Name is a required field on TestInfo, should prevent null
                                    // TestInfo
                                    CriterionUtils.buildCriterion(
                                        "name", Condition.EXISTS, false)))))));
  }
}
