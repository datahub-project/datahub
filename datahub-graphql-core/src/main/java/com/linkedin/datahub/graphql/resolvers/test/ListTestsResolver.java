package com.linkedin.datahub.graphql.resolvers.test;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListTestsInput;
import com.linkedin.datahub.graphql.generated.ListTestsResult;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestFetcher;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.test.TestUtils.canManageTests;


/**
 * Resolver used for listing all Tests defined within DataHub. Requires the MANAGE_DOMAINS platform privilege.
 */
public class ListTestsResolver implements DataFetcher<CompletableFuture<ListTestsResult>> {

  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;

  private final TestFetcher _testFetcher;

  public ListTestsResolver(final EntityService entityService, final EntitySearchService entitySearchService) {
    _testFetcher = new TestFetcher(entityService, entitySearchService);
  }

  @Override
  public CompletableFuture<ListTestsResult> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(() -> {

      if (canManageTests(context)) {
        final ListTestsInput input = bindArgument(environment.getArgument("input"), ListTestsInput.class);
        final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
        final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
        final String query = input.getQuery() == null ? "" : input.getQuery();

        try {
          // First, get all group Urns.
          final TestFetcher.TestFetchResult testFetchResult = _testFetcher.fetch(start, count, query);

          // Now that we have entities we can bind this to a result.
          final ListTestsResult result = new ListTestsResult();
          result.setStart(start);
          result.setCount(count);
          result.setTotal(testFetchResult.getTotal());
          result.setTests(mapUnresolvedTests(testFetchResult.getTests()));
          return result;
        } catch (Exception e) {
          throw new RuntimeException("Failed to list tests", e);
        }
      }
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    });
  }

  // This method maps urns returned from the list endpoint into Partial Test objects which will be resolved be a separate Batch resolver.
  private List<Test> mapUnresolvedTests(final List<TestFetcher.Test> tests) {
    final List<Test> results = new ArrayList<>();
    for (final TestFetcher.Test test : tests) {
      final Test unresolvedTest = new Test();
      unresolvedTest.setUrn(test.getUrn().toString());
      unresolvedTest.setType(EntityType.TEST);
      results.add(unresolvedTest);
    }
    return results;
  }
}
