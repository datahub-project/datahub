package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import graphql.GraphqlErrorBuilder;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetchingEnvironment;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class OmitUnrepresentableEntitiesTest {

  private static final DataFetcherResult<Object> LOADED =
      DataFetcherResult.newResult().data("loaded").build();
  private static final DataFetcherResult<Object> WITHOUT_DATA =
      DataFetcherResult.newResult().build();
  private static final DataFetcherResult<Object> WITH_ERRORS =
      DataFetcherResult.newResult()
          .error(GraphqlErrorBuilder.newError().message("failed").build())
          .build();

  @AfterMethod
  public void resetHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @Test
  public void testTolerantDropsEntriesWithoutDataButKeepsErrors() throws Exception {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);

    List<Object> result =
        OmitUnrepresentableEntities.from(resolverReturning(loadedEntries()))
            .get(mock(DataFetchingEnvironment.class))
            .get();

    assertEquals(result, List.of(LOADED, WITH_ERRORS));
  }

  @Test
  public void testStrictReturnsTheLoadedListUnchanged() throws Exception {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    CompletableFuture<List<Object>> loaded = CompletableFuture.completedFuture(loadedEntries());

    assertSame(
        OmitUnrepresentableEntities.from(resolverReturning(loaded))
            .get(mock(DataFetchingEnvironment.class)),
        loaded);
  }

  private static List<Object> loadedEntries() {
    return Arrays.asList(LOADED, null, WITHOUT_DATA, WITH_ERRORS);
  }

  private static LoadableTypeBatchResolver<Object, String> resolverReturning(List<Object> loaded) {
    return resolverReturning(CompletableFuture.completedFuture(loaded));
  }

  @SuppressWarnings("unchecked")
  private static LoadableTypeBatchResolver<Object, String> resolverReturning(
      CompletableFuture<List<Object>> loaded) {
    LoadableTypeBatchResolver<Object, String> resolver = mock(LoadableTypeBatchResolver.class);
    when(resolver.get(any())).thenReturn(loaded);
    return resolver;
  }
}
