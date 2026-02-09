package com.linkedin.datahub.graphql.resolvers.jobs;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataProcessInstance;
import com.linkedin.datahub.graphql.generated.DataProcessInstanceResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataprocess.DataProcessInstanceProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link ExecutionRunsResolver}.
 *
 * <p>Critical paths tested:
 *
 * <ul>
 *   <li>Filter construction with parentTemplate and hasRunEvents fields
 *   <li>Sort criteria (descending by created time)
 *   <li>Pagination handling
 *   <li>Works for both DataFlow and DataJob parent entities
 *   <li>Empty results handling
 *   <li>Error propagation
 * </ul>
 */
public class ExecutionRunsResolverTest {

  private static final String TEST_DATA_FLOW_URN =
      "urn:li:dataFlow:(azure-data-factory,test-factory.test-pipeline,DEV)";
  private static final String TEST_DATA_JOB_URN =
      "urn:li:dataJob:(urn:li:dataFlow:(airflow,test_dag,PROD),test_task)";
  private static final String TEST_DPI_URN_1 = "urn:li:dataProcessInstance:run-123";
  private static final String TEST_DPI_URN_2 = "urn:li:dataProcessInstance:run-456";

  private EntityClient mockClient;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;
  private OperationContext mockOpContext;

  @BeforeMethod
  public void setup() {
    mockClient = Mockito.mock(EntityClient.class);
    mockContext = Mockito.mock(QueryContext.class);
    mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    mockOpContext = Mockito.mock(OperationContext.class);

    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testGetRunsForDataFlowSuccess() throws Exception {
    // Setup parent DataFlow entity
    DataFlow dataFlow = new DataFlow();
    dataFlow.setUrn(TEST_DATA_FLOW_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(dataFlow);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("start"), eq(0))).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("count"), eq(20))).thenReturn(10);

    Urn dpiUrn1 = Urn.createFromString(TEST_DPI_URN_1);
    Urn dpiUrn2 = Urn.createFromString(TEST_DPI_URN_2);

    // Mock search results
    mockSearchResults(ImmutableList.of(dpiUrn1, dpiUrn2), 2);

    // Mock entity responses
    mockEntityResponses(
        ImmutableMap.of(
            dpiUrn1, createDpiEntityResponse(dpiUrn1, "Run 1"),
            dpiUrn2, createDpiEntityResponse(dpiUrn2, "Run 2")));

    ExecutionRunsResolver resolver = new ExecutionRunsResolver(mockClient);
    DataProcessInstanceResult result = resolver.get(mockEnv).get();

    // Verify result
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getCount(), 2); // Count reflects actual page size from search result
    assertEquals(result.getStart(), 0);
    assertEquals(result.getRuns().size(), 2);

    // Verify the runs contain expected data
    DataProcessInstance run1 = result.getRuns().get(0);
    assertEquals(run1.getUrn(), TEST_DPI_URN_1);
    assertEquals(run1.getName(), "Run 1");
    assertEquals(run1.getType(), EntityType.DATA_PROCESS_INSTANCE);
  }

  @Test
  public void testGetRunsForDataJobSuccess() throws Exception {
    // Setup parent DataJob entity
    DataJob dataJob = new DataJob();
    dataJob.setUrn(TEST_DATA_JOB_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(dataJob);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("start"), eq(0))).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("count"), eq(20))).thenReturn(5);

    Urn dpiUrn1 = Urn.createFromString(TEST_DPI_URN_1);

    // Mock search results
    mockSearchResults(ImmutableList.of(dpiUrn1), 1);

    // Mock entity responses
    mockEntityResponses(ImmutableMap.of(dpiUrn1, createDpiEntityResponse(dpiUrn1, "Task Run 1")));

    ExecutionRunsResolver resolver = new ExecutionRunsResolver(mockClient);
    DataProcessInstanceResult result = resolver.get(mockEnv).get();

    // Verify result
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getRuns().size(), 1);
    assertEquals(result.getRuns().get(0).getName(), "Task Run 1");
  }

  @Test
  public void testFilterAndSortCriteriaForDataFlow() throws Exception {
    // Setup parent DataFlow entity
    DataFlow dataFlow = new DataFlow();
    dataFlow.setUrn(TEST_DATA_FLOW_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(dataFlow);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("start"), eq(0))).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("count"), eq(20))).thenReturn(10);

    // Mock empty results - we just want to verify filter/sort
    mockSearchResults(Collections.emptyList(), 0);
    mockEntityResponses(Collections.emptyMap());

    ExecutionRunsResolver resolver = new ExecutionRunsResolver(mockClient);
    resolver.get(mockEnv).get();

    // Capture and verify the filter
    ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    ArgumentCaptor<List<SortCriterion>> sortCaptor = ArgumentCaptor.forClass(List.class);

    verify(mockClient)
        .filter(
            eq(mockOpContext),
            eq(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME),
            filterCaptor.capture(),
            sortCaptor.capture(),
            eq(0),
            eq(10));

    // Verify filter has parentTemplate and hasRunEvents criteria
    Filter capturedFilter = filterCaptor.getValue();
    assertNotNull(capturedFilter.getOr());
    assertEquals(capturedFilter.getOr().size(), 1);
    assertEquals(capturedFilter.getOr().get(0).getAnd().size(), 2);

    // Verify sort is by created time descending
    List<SortCriterion> capturedSort = sortCaptor.getValue();
    assertEquals(capturedSort.size(), 1);
    assertEquals(capturedSort.get(0).getField(), "created");
    assertEquals(capturedSort.get(0).getOrder(), SortOrder.DESCENDING);
  }

  @Test
  public void testPaginationParameters() throws Exception {
    // Setup parent entity
    DataFlow dataFlow = new DataFlow();
    dataFlow.setUrn(TEST_DATA_FLOW_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(dataFlow);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("start"), eq(0))).thenReturn(20);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("count"), eq(20))).thenReturn(50);

    // Mock search results with pagination info
    Mockito.when(
            mockClient.filter(
                eq(mockOpContext),
                eq(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME),
                any(Filter.class),
                anyList(),
                eq(20),
                eq(50)))
        .thenReturn(
            new SearchResult()
                .setFrom(20)
                .setPageSize(50)
                .setNumEntities(100)
                .setEntities(new SearchEntityArray()));

    mockEntityResponses(Collections.emptyMap());

    ExecutionRunsResolver resolver = new ExecutionRunsResolver(mockClient);
    DataProcessInstanceResult result = resolver.get(mockEnv).get();

    // Verify pagination values in result
    assertEquals(result.getStart(), 20);
    assertEquals(result.getCount(), 50);
    assertEquals(result.getTotal(), 100);
  }

  @Test
  public void testEmptyResults() throws Exception {
    // Setup parent entity
    DataFlow dataFlow = new DataFlow();
    dataFlow.setUrn(TEST_DATA_FLOW_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(dataFlow);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("start"), eq(0))).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("count"), eq(20))).thenReturn(10);

    // Mock empty search results
    mockSearchResults(Collections.emptyList(), 0);
    mockEntityResponses(Collections.emptyMap());

    ExecutionRunsResolver resolver = new ExecutionRunsResolver(mockClient);
    DataProcessInstanceResult result = resolver.get(mockEnv).get();

    // Verify empty result
    assertEquals(result.getTotal(), 0);
    assertEquals(result.getRuns().size(), 0);
  }

  @Test
  public void testExceptionHandling() throws Exception {
    // Setup parent entity
    DataFlow dataFlow = new DataFlow();
    dataFlow.setUrn(TEST_DATA_FLOW_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(dataFlow);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("start"), eq(0))).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("count"), eq(20))).thenReturn(10);

    // Mock search to throw exception
    Mockito.when(
            mockClient.filter(
                any(OperationContext.class),
                eq(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME),
                any(Filter.class),
                anyList(),
                anyInt(),
                anyInt()))
        .thenThrow(new RemoteInvocationException("Test exception"));

    ExecutionRunsResolver resolver = new ExecutionRunsResolver(mockClient);

    try {
      resolver.get(mockEnv).get();
      fail("Expected exception to be thrown");
    } catch (Exception e) {
      // Verify the exception is wrapped in a RuntimeException
      assertTrue(e.getCause() instanceof RuntimeException);
      assertTrue(e.getCause().getMessage().contains("Failed to retrieve"));
    }
  }

  @Test
  public void testHandlesNullEntityInBatchGet() throws Exception {
    // Setup parent entity
    DataFlow dataFlow = new DataFlow();
    dataFlow.setUrn(TEST_DATA_FLOW_URN);
    Mockito.when(mockEnv.getSource()).thenReturn(dataFlow);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("start"), eq(0))).thenReturn(0);
    Mockito.when(mockEnv.getArgumentOrDefault(eq("count"), eq(20))).thenReturn(10);

    Urn dpiUrn1 = Urn.createFromString(TEST_DPI_URN_1);
    Urn dpiUrn2 = Urn.createFromString(TEST_DPI_URN_2);

    // Mock search returns 2 URNs
    mockSearchResults(ImmutableList.of(dpiUrn1, dpiUrn2), 2);

    // Mock batchGetV2 returns only 1 entity (simulating a deleted entity)
    mockEntityResponses(ImmutableMap.of(dpiUrn1, createDpiEntityResponse(dpiUrn1, "Run 1")));

    ExecutionRunsResolver resolver = new ExecutionRunsResolver(mockClient);
    DataProcessInstanceResult result = resolver.get(mockEnv).get();

    // Verify only the existing entity is returned (null filtered out)
    assertEquals(result.getTotal(), 2); // Total from search
    assertEquals(result.getRuns().size(), 1); // Only 1 non-null entity
    assertEquals(result.getRuns().get(0).getName(), "Run 1");
  }

  // Helper methods

  private void mockSearchResults(List<Urn> urns, int total) throws Exception {
    SearchEntityArray entities = new SearchEntityArray();
    urns.forEach(urn -> entities.add(new SearchEntity().setEntity(urn)));

    Mockito.when(
            mockClient.filter(
                any(OperationContext.class),
                eq(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME),
                any(Filter.class),
                anyList(),
                anyInt(),
                anyInt()))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(urns.size())
                .setNumEntities(total)
                .setEntities(entities));
  }

  private void mockEntityResponses(Map<Urn, EntityResponse> responses) throws Exception {
    Mockito.when(
            mockClient.batchGetV2(
                any(OperationContext.class),
                eq(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME),
                anySet(),
                eq(null)))
        .thenReturn(responses);
  }

  private EntityResponse createDpiEntityResponse(Urn urn, String name) {
    Map<String, com.linkedin.entity.EnvelopedAspect> aspects = new HashMap<>();

    // Create DataProcessInstanceProperties aspect
    DataProcessInstanceProperties props =
        new DataProcessInstanceProperties()
            .setName(name)
            .setCreated(new AuditStamp().setTime(System.currentTimeMillis()).setActor(urn))
            .setCustomProperties(new StringMap());

    aspects.put(
        Constants.DATA_PROCESS_INSTANCE_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect().setValue(new Aspect(props.data())));

    return new EntityResponse()
        .setEntityName(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME)
        .setUrn(urn)
        .setAspects(new EnvelopedAspectMap(aspects));
  }
}
