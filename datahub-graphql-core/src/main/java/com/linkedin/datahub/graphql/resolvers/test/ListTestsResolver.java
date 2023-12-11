package com.linkedin.datahub.graphql.resolvers.test;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.test.TestUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListTestsInput;
import com.linkedin.datahub.graphql.generated.ListTestsResult;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
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

  public ListTestsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListTestsResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    return CompletableFuture.supplyAsync(
        () -> {
          if (canManageTests(context)) {
            final ListTestsInput input =
                bindArgument(environment.getArgument("input"), ListTestsInput.class);
            final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
            final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
            final String query = input.getQuery() == null ? "" : input.getQuery();

            try {
              // First, get all group Urns.
              final SearchResult gmsResult =
                  _entityClient.search(
                      Constants.TEST_ENTITY_NAME,
                      query,
                      Collections.emptyMap(),
                      start,
                      count,
                      context.getAuthentication(),
                      new SearchFlags().setFulltext(true));

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
        });
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
}
