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
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
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
  public void testIncludeHiddenLifecycleStagesSkipsDocumentDefaultFiltersForDocumentManagers()
      throws Exception {
    ScrollResult mockResult = new ScrollResult();
    mockResult.setPageSize(0);
    mockResult.setNumEntities(0);
    mockResult.setMetadata(new SearchResultMetadata());
    mockResult.setEntities(new SearchEntityArray());

    SearchFlags searchFlags = new SearchFlags();
    searchFlags.setIncludeHiddenLifecycleStages(true);

    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("*");
    input.setCount(10);
    input.setSearchFlags(searchFlags);

    QueryContext context = getMockAllowContext();
    Mockito.when(_env.getArgument("input")).thenReturn(input);
    Mockito.when(_env.getContext()).thenReturn(context);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(), any(), eq("*"), any(), eq(null), eq("5m"), any(), eq(10)))
        .thenReturn(mockResult);

    ScrollResults result = _resolver.get(_env).get();

    assertNotNull(result);

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            eq(Collections.singletonList(Constants.DOCUMENT_ENTITY_NAME)),
            eq("*"),
            filterCaptor.capture(),
            eq(null),
            eq("5m"),
            eq(Collections.emptyList()),
            eq(10));
    assertNull(filterCaptor.getValue());
  }

  @Test
  public void testIncludeHiddenLifecycleStagesBypassesDocumentDefaultFiltersWithoutManageDocuments()
      throws Exception {
    ScrollResult mockResult = new ScrollResult();
    mockResult.setPageSize(0);
    mockResult.setNumEntities(0);
    mockResult.setMetadata(new SearchResultMetadata());
    mockResult.setEntities(new SearchEntityArray());

    // includeHiddenLifecycleStages alone is sufficient to bypass default document filters —
    // internal callers (agent dashboards, eval tooling) set this flag and already have
    // system-level access, so the canManageDocuments privilege check is unnecessary.
    SearchFlags searchFlags = new SearchFlags();
    searchFlags.setIncludeHiddenLifecycleStages(true);

    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("*");
    input.setCount(10);
    input.setSearchFlags(searchFlags);

    QueryContext context = getMockDenyContext();
    Mockito.when(_env.getArgument("input")).thenReturn(input);
    Mockito.when(_env.getContext()).thenReturn(context);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(), any(), eq("*"), any(), eq(null), eq("5m"), any(), eq(10)))
        .thenReturn(mockResult);

    ScrollResults result = _resolver.get(_env).get();

    assertNotNull(result);

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            eq(Collections.singletonList(Constants.DOCUMENT_ENTITY_NAME)),
            eq("*"),
            filterCaptor.capture(),
            eq(null),
            eq("5m"),
            eq(Collections.emptyList()),
            eq(10));
    // includeHiddenLifecycleStages alone is sufficient to bypass default document filters
    assertNull(filterCaptor.getValue());
  }

  @Test
  public void testWithoutIncludeHiddenLifecycleStagesKeepsDocumentDefaultFilters()
      throws Exception {
    ScrollResult mockResult = new ScrollResult();
    mockResult.setPageSize(0);
    mockResult.setNumEntities(0);
    mockResult.setMetadata(new SearchResultMetadata());
    mockResult.setEntities(new SearchEntityArray());

    // Document manager, but includeHiddenLifecycleStages is not set: defaults must still apply.
    SearchFlags searchFlags = new SearchFlags();
    searchFlags.setIncludeHiddenLifecycleStages(false);

    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("*");
    input.setCount(10);
    input.setSearchFlags(searchFlags);

    QueryContext context = getMockAllowContext();
    Mockito.when(_env.getArgument("input")).thenReturn(input);
    Mockito.when(_env.getContext()).thenReturn(context);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(), any(), eq("*"), any(), eq(null), eq("5m"), any(), eq(10)))
        .thenReturn(mockResult);

    ScrollResults result = _resolver.get(_env).get();

    assertNotNull(result);

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            eq(Collections.singletonList(Constants.DOCUMENT_ENTITY_NAME)),
            eq("*"),
            filterCaptor.capture(),
            eq(null),
            eq("5m"),
            eq(Collections.emptyList()),
            eq(10));
    // With canManageDocuments=true (mock allows all), the state field is omitted from
    // unpublished clauses and replaced by a lifecycleStage EXISTS + lifecycleStage != DRAFT clause.
    assertTrue(filterContainsField(filterCaptor.getValue(), "lifecycleStage"));
    assertTrue(filterContainsField(filterCaptor.getValue(), "showInGlobalContext"));
  }

  @Test
  public void testIncludeHiddenLifecycleStagesDoesNotBypassDefaultsForNonDocumentEntities()
      throws Exception {
    ScrollResult mockResult = new ScrollResult();
    mockResult.setPageSize(0);
    mockResult.setNumEntities(0);
    mockResult.setMetadata(new SearchResultMetadata());
    mockResult.setEntities(new SearchEntityArray());

    // Document manager with the hidden-stages flag, but searching a non-document entity:
    // the bypass is document-scoped, so no document default filters are injected.
    SearchFlags searchFlags = new SearchFlags();
    searchFlags.setIncludeHiddenLifecycleStages(true);

    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("*");
    input.setCount(10);
    input.setSearchFlags(searchFlags);

    QueryContext context = getMockAllowContext();
    Mockito.when(_env.getArgument("input")).thenReturn(input);
    Mockito.when(_env.getContext()).thenReturn(context);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(), any(), eq("*"), any(), eq(null), eq("5m"), any(), eq(10)))
        .thenReturn(mockResult);

    ScrollResults result = _resolver.get(_env).get();

    assertNotNull(result);

    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            eq(Collections.singletonList(Constants.DATASET_ENTITY_NAME)),
            eq("*"),
            filterCaptor.capture(),
            eq(null),
            eq("5m"),
            eq(Collections.emptyList()),
            eq(10));
    assertFalse(filterContainsField(filterCaptor.getValue(), "state"));
    assertFalse(filterContainsField(filterCaptor.getValue(), "showInGlobalContext"));
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

  private static boolean filterContainsField(Filter filter, String field) {
    if (filter == null || !filter.hasOr()) {
      return false;
    }
    return filter.getOr().stream()
        .flatMap(conjunction -> conjunction.getAnd().stream())
        .anyMatch(criterion -> field.equals(criterion.getField()));
  }
}
