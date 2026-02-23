package com.linkedin.datahub.graphql.resolvers.search;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ScrollAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ScrollAcrossEntitiesResolverTest {

  private EntityClient _entityClient;
  private ViewService _viewService;
  private DataFetchingEnvironment _env;
  private ScrollAcrossEntitiesResolver _resolver;

  @BeforeMethod
  public void setup() {
    _entityClient = Mockito.mock(EntityClient.class);
    _viewService = Mockito.mock(ViewService.class);
    _env = Mockito.mock(DataFetchingEnvironment.class);
    _resolver = new ScrollAcrossEntitiesResolver(_entityClient, _viewService);
  }

  @Test
  public void testGetSuccessWithExplicitTypes() throws Exception {
    ScrollResult mockResult = new ScrollResult();
    mockResult.setScrollId("scroll-1");
    mockResult.setPageSize(2);
    mockResult.setNumEntities(2);
    mockResult.setMetadata(new SearchResultMetadata());
    mockResult.setEntities(new SearchEntityArray());

    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("test");
    input.setCount(5);

    Mockito.when(_env.getArgument("input")).thenReturn(input);
    QueryContext context = getMockAllowContext();
    Mockito.when(_env.getContext()).thenReturn(context);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(),
                eq(Collections.singletonList(Constants.DATASET_ENTITY_NAME)),
                eq("test"),
                any(),
                eq(null),
                eq("5m"),
                eq(Collections.emptyList()),
                eq(5)))
        .thenReturn(mockResult);

    ScrollResults result = _resolver.get(_env).get();

    assertNotNull(result);
    assertEquals(result.getNextScrollId(), "scroll-1");
    assertEquals(result.getCount(), 2);
    assertEquals(result.getTotal(), 2);
    verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            eq(Collections.singletonList(Constants.DATASET_ENTITY_NAME)),
            eq("test"),
            any(),
            eq(null),
            eq("5m"),
            eq(Collections.emptyList()),
            eq(5));
  }

  @Test
  public void testGetSuccessWithScrollIdAndKeepAlive() throws Exception {
    ScrollResult mockResult = new ScrollResult();
    mockResult.setPageSize(0);
    mockResult.setNumEntities(0);
    mockResult.setMetadata(new SearchResultMetadata());
    mockResult.setEntities(new SearchEntityArray());

    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("*");
    input.setScrollId("next-scroll-id");
    input.setKeepAlive("2m");
    input.setCount(10);

    QueryContext context = getMockAllowContext();
    Mockito.when(_env.getArgument("input")).thenReturn(input);
    Mockito.when(_env.getContext()).thenReturn(context);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(), any(), eq("*"), any(), eq("next-scroll-id"), eq("2m"), any(), eq(10)))
        .thenReturn(mockResult);

    ScrollResults result = _resolver.get(_env).get();

    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
  }

  @Test
  public void testGetExceptionThrowsRuntimeException() throws Exception {
    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("fail");
    input.setCount(10);

    QueryContext context = getMockAllowContext();
    Mockito.when(_env.getArgument("input")).thenReturn(input);
    Mockito.when(_env.getContext()).thenReturn(context);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(), any(), eq("fail"), any(), any(), any(), any(), any()))
        .thenThrow(new RemoteInvocationException("scroll failed"));

    try {
      _resolver.get(_env).join();
      Assert.fail("expected CompletionException");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("Failed to execute search"));
    }
  }
}
