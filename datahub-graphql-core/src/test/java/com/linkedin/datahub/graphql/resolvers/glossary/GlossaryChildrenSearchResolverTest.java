package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ScrollAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlossaryChildrenSearchResolverTest {
  private static final String TEST_GLOSSARY_NODE_URN = "urn:li:glossaryNode:test-id";

  private EntityClient _entityClient;
  private ViewService _viewService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private GlossaryChildrenSearchResolver _resolver;
  private Entity _entity;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _viewService = Mockito.mock(ViewService.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _entity = Mockito.mock(Entity.class);
    Mockito.when(_entity.getUrn()).thenReturn(TEST_GLOSSARY_NODE_URN);
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(_entity);

    _resolver = new GlossaryChildrenSearchResolver(_entityClient, _viewService);
  }

  @Test
  public void testGetSuccess() throws Exception {
    // Setup mock scroll results
    ScrollResult mockScrollResult = new ScrollResult();
    mockScrollResult.setScrollId("test-scroll-id");
    mockScrollResult.setPageSize(2);
    mockScrollResult.setNumEntities(2);

    // Setup mock input
    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.GLOSSARY_TERM));
    input.setQuery("test");
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Setup mock search response
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(),
                eq(Collections.singletonList(Constants.GLOSSARY_TERM_ENTITY_NAME)),
                eq("test"),
                any(),
                eq(null),
                eq("5m"),
                eq(Collections.emptyList()),
                eq(10),
                eq(Collections.emptyList())))
        .thenReturn(mockScrollResult);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    ScrollResults result = _resolver.get(_dataFetchingEnvironment).get();

    // Verify results
    assertEquals(result.getNextScrollId(), "test-scroll-id");
    assertEquals(result.getCount(), 2);
    assertEquals(result.getTotal(), 2);
  }

  @Test
  public void testGetEmptyResults() throws Exception {
    // Setup mock scroll results with no entities
    ScrollResult mockScrollResult = new ScrollResult();
    mockScrollResult.setPageSize(0);
    mockScrollResult.setNumEntities(0);

    // Setup mock input
    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.GLOSSARY_TERM));
    input.setQuery("test");
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Setup mock search response
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(),
                eq(Collections.singletonList(Constants.GLOSSARY_TERM_ENTITY_NAME)),
                eq("test"),
                any(),
                eq(null),
                eq("5m"),
                eq(Collections.emptyList()),
                eq(10),
                eq(Collections.emptyList())))
        .thenReturn(mockScrollResult);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    ScrollResults result = _resolver.get(_dataFetchingEnvironment).get();

    // Verify results
    assertNull(result.getNextScrollId());
    assertEquals(result.getCount(), 0);
    assertEquals(result.getTotal(), 0);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Setup mock input
    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.GLOSSARY_TERM));
    input.setQuery("test");
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Execute resolver with unauthorized context
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    // Setup mock input
    ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.GLOSSARY_TERM));
    input.setQuery("test");
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Setup mock to throw exception
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(),
                eq(Collections.singletonList(Constants.GLOSSARY_TERM_ENTITY_NAME)),
                eq("test"),
                any(),
                eq(null),
                eq("5m"),
                eq(Collections.emptyList()),
                eq(10),
                eq(Collections.emptyList())))
        .thenThrow(new RemoteInvocationException("Failed to search"));

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }
}
