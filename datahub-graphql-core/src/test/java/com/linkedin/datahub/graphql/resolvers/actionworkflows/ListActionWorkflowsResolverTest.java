package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.ActionWorkflowService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ListActionWorkflowsResolverTest {

  private static final String TEST_USER_URN = "urn:li:corpuser:test";

  @Mock private EntityClient mockEntityClient;

  @Mock private EntityService mockEntityService;

  @Mock private ActionWorkflowService mockActionWorkflowService;

  @Mock private QueryContext mockQueryContext;

  @Mock private OperationContext mockOperationContext;

  @Mock private DataFetchingEnvironment mockDataFetchingEnvironment;

  private ListActionWorkflowsResolver resolver;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    resolver = new ListActionWorkflowsResolver(mockActionWorkflowService);

    when(mockQueryContext.getActorUrn()).thenReturn(TEST_USER_URN);
    when(mockQueryContext.getOperationContext()).thenReturn(mockOperationContext);

    // Mock the service to return empty results - count=0 means no items returned
    try {
      ActionWorkflowService.ActionWorkflowListResult emptyResult =
          new ActionWorkflowService.ActionWorkflowListResult(
              0, 0, 0, java.util.Collections.emptyList());
      when(mockActionWorkflowService.listActionWorkflows(
              any(OperationContext.class), eq(0), eq(20), any(), any(), any(), any()))
          .thenReturn(emptyResult);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testListWorkflowsDefaultInput() throws Exception {
    // GIVEN
    ListActionWorkflowsInput input = new ListActionWorkflowsInput();
    // Use default values: start=0, count=20

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN
    CompletableFuture<ListActionWorkflowResult> futureResult =
        resolver.get(mockDataFetchingEnvironment);
    ListActionWorkflowResult result = futureResult.get();

    // THEN
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getStart(), Integer.valueOf(0));
    Assert.assertEquals(result.getCount(), Integer.valueOf(0));
    Assert.assertEquals(result.getTotal(), Integer.valueOf(0));
    Assert.assertEquals(result.getWorkflows().size(), 0);

    // Verify the service was called with GraphQL defaults (0, 20)
    verify(mockActionWorkflowService)
        .listActionWorkflows(
            eq(mockOperationContext),
            eq(0), // start defaults to 0 in GraphQL schema
            eq(20), // count defaults to 20 in GraphQL schema
            eq(null), // type
            eq(null), // customType
            eq(null), // entrypointType
            eq(null) // entityType
            );
  }

  @Test
  public void testListWorkflowsWithCustomPagination() throws Exception {
    // GIVEN
    ListActionWorkflowsInput input = new ListActionWorkflowsInput();
    input.setStart(10);
    input.setCount(5);

    // Mock service to return result with the requested pagination
    ActionWorkflowService.ActionWorkflowListResult paginatedResult =
        new ActionWorkflowService.ActionWorkflowListResult(
            10, 5, 100, java.util.Collections.emptyList());
    when(mockActionWorkflowService.listActionWorkflows(
            any(OperationContext.class), eq(10), eq(5), any(), any(), any(), any()))
        .thenReturn(paginatedResult);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN
    CompletableFuture<ListActionWorkflowResult> futureResult =
        resolver.get(mockDataFetchingEnvironment);
    ListActionWorkflowResult result = futureResult.get();

    // THEN
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getStart(), Integer.valueOf(10));
    Assert.assertEquals(result.getCount(), Integer.valueOf(5));
    Assert.assertEquals(result.getTotal(), Integer.valueOf(100));
    Assert.assertEquals(result.getWorkflows().size(), 0);

    // Verify the service was called with correct parameters
    verify(mockActionWorkflowService)
        .listActionWorkflows(
            eq(mockOperationContext),
            eq(10), // start
            eq(5), // count
            eq(null), // type
            eq(null), // customType
            eq(null), // entrypointType
            eq(null) // entityType
            );
  }

  @Test
  public void testListWorkflowsWithCategoryFilter() throws Exception {
    // GIVEN
    ListActionWorkflowsInput input = new ListActionWorkflowsInput();
    input.setCategory(ActionWorkflowCategory.ACCESS);

    // Mock service to return result for the category filter - count=0 means no items returned
    ActionWorkflowService.ActionWorkflowListResult categoryResult =
        new ActionWorkflowService.ActionWorkflowListResult(
            0, 0, 5, java.util.Collections.emptyList());
    when(mockActionWorkflowService.listActionWorkflows(
            any(OperationContext.class), eq(0), eq(20), any(), any(), any(), any()))
        .thenReturn(categoryResult);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN
    CompletableFuture<ListActionWorkflowResult> futureResult =
        resolver.get(mockDataFetchingEnvironment);
    ListActionWorkflowResult result = futureResult.get();

    // THEN
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getStart(), Integer.valueOf(0));
    Assert.assertEquals(result.getCount(), Integer.valueOf(0)); // 0 items returned
    Assert.assertEquals(result.getTotal(), Integer.valueOf(5));
    Assert.assertEquals(result.getWorkflows().size(), 0);

    // Verify the service was called with the type filter and defaults
    verify(mockActionWorkflowService)
        .listActionWorkflows(
            eq(mockOperationContext),
            eq(0), // start defaults to 0
            eq(20), // count defaults to 20
            any(), // type (converted from GraphQL enum)
            eq(null), // customType
            eq(null), // entrypointType
            eq(null) // entityType
            );
  }

  @Test
  public void testListWorkflowsWithCustomCategoryFilter() throws Exception {
    // GIVEN
    ListActionWorkflowsInput input = new ListActionWorkflowsInput();
    input.setCategory(ActionWorkflowCategory.CUSTOM);
    input.setCustomCategory("Custom Category");

    // Mock service to return result for the custom category filter - count=0 means no items
    // returned
    ActionWorkflowService.ActionWorkflowListResult customResult =
        new ActionWorkflowService.ActionWorkflowListResult(
            0, 0, 2, java.util.Collections.emptyList());
    when(mockActionWorkflowService.listActionWorkflows(
            any(OperationContext.class), eq(0), eq(20), any(), eq("Custom Category"), any(), any()))
        .thenReturn(customResult);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN
    CompletableFuture<ListActionWorkflowResult> futureResult =
        resolver.get(mockDataFetchingEnvironment);
    ListActionWorkflowResult result = futureResult.get();

    // THEN
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getStart(), Integer.valueOf(0));
    Assert.assertEquals(result.getCount(), Integer.valueOf(0)); // 0 items returned
    Assert.assertEquals(result.getTotal(), Integer.valueOf(2));
    Assert.assertEquals(result.getWorkflows().size(), 0);

    // Verify the service was called with the custom type filter and defaults
    verify(mockActionWorkflowService)
        .listActionWorkflows(
            eq(mockOperationContext),
            eq(0), // start defaults to 0
            eq(20), // count defaults to 20
            any(), // type
            eq("Custom Category"), // customType
            eq(null), // entrypointType
            eq(null) // entityType
            );
  }

  @Test
  public void testListWorkflowsWithEntrypointTypeFilter() throws Exception {
    // GIVEN
    ListActionWorkflowsInput input = new ListActionWorkflowsInput();
    input.setEntrypointType(ActionWorkflowEntrypointType.ENTITY_PROFILE);

    // Mock service to return result for the entrypoint filter - count=0 means no items returned
    ActionWorkflowService.ActionWorkflowListResult entrypointResult =
        new ActionWorkflowService.ActionWorkflowListResult(
            0, 0, 3, java.util.Collections.emptyList());
    when(mockActionWorkflowService.listActionWorkflows(
            any(OperationContext.class), eq(0), eq(20), any(), any(), any(), any()))
        .thenReturn(entrypointResult);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN
    CompletableFuture<ListActionWorkflowResult> futureResult =
        resolver.get(mockDataFetchingEnvironment);
    ListActionWorkflowResult result = futureResult.get();

    // THEN
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getStart(), Integer.valueOf(0));
    Assert.assertEquals(result.getCount(), Integer.valueOf(0)); // 0 items returned
    Assert.assertEquals(result.getTotal(), Integer.valueOf(3));
    Assert.assertEquals(result.getWorkflows().size(), 0);

    // Verify the service was called with the entrypoint type filter and defaults
    verify(mockActionWorkflowService)
        .listActionWorkflows(
            eq(mockOperationContext),
            eq(0), // start defaults to 0
            eq(20), // count defaults to 20
            eq(null), // type
            eq(null), // customType
            any(), // entrypointType (converted from GraphQL enum)
            eq(null) // entityType
            );
  }

  @Test
  public void testListWorkflowsWithEntityTypeFilter() throws Exception {
    // GIVEN
    ListActionWorkflowsInput input = new ListActionWorkflowsInput();
    input.setEntityType(EntityType.DATASET);

    // Mock service to return result for the entity type filter - count=0 means no items returned
    ActionWorkflowService.ActionWorkflowListResult entityTypeResult =
        new ActionWorkflowService.ActionWorkflowListResult(
            0, 0, 7, java.util.Collections.emptyList());
    when(mockActionWorkflowService.listActionWorkflows(
            any(OperationContext.class),
            eq(0),
            eq(20),
            any(),
            any(),
            any(),
            eq("urn:li:entityType:datahub.dataset")))
        .thenReturn(entityTypeResult);

    when(mockDataFetchingEnvironment.getArgument("input")).thenReturn(input);
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockQueryContext);

    // WHEN
    CompletableFuture<ListActionWorkflowResult> futureResult =
        resolver.get(mockDataFetchingEnvironment);
    ListActionWorkflowResult result = futureResult.get();

    // THEN
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getStart(), Integer.valueOf(0));
    Assert.assertEquals(result.getCount(), Integer.valueOf(0)); // 0 items returned
    Assert.assertEquals(result.getTotal(), Integer.valueOf(7));
    Assert.assertEquals(result.getWorkflows().size(), 0);

    // Verify the service was called with the entity type filter and defaults
    verify(mockActionWorkflowService)
        .listActionWorkflows(
            eq(mockOperationContext),
            eq(0), // start defaults to 0
            eq(20), // count defaults to 20
            eq(null), // type
            eq(null), // customType
            eq(null), // entrypointType
            eq("urn:li:entityType:datahub.dataset") // entityType
            );
  }
}
