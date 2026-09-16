package com.linkedin.datahub.graphql.types.module;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubPageModule;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.module.DataHubPageModuleProperties;
import com.linkedin.module.DataHubPageModuleType;
import com.linkedin.module.DataHubPageModuleVisibility;
import com.linkedin.module.PageModuleScope;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PageModuleTypeTest {

  private PageModuleType pageModuleType;
  private EntityClient mockEntityClient;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setUp() {
    mockEntityClient = mock(EntityClient.class);
    mockQueryContext = mock(QueryContext.class);
    pageModuleType = new PageModuleType(mockEntityClient);
    // Mock getOperationContext to avoid NPE
    OperationContext dummyOpContext = mock(OperationContext.class);
    when(mockQueryContext.getOperationContext()).thenReturn(dummyOpContext);
  }

  @Test
  public void testType() {
    assertEquals(pageModuleType.type(), EntityType.DATAHUB_PAGE_MODULE);
  }

  @Test
  public void testObjectClass() {
    assertEquals(pageModuleType.objectClass(), DataHubPageModule.class);
  }

  @Test
  public void testGetKeyProvider() {
    // Create a mock entity
    DataHubPageModule mockEntity = new DataHubPageModule();
    mockEntity.setUrn("urn:li:dataHubPageModule:test");

    String result = pageModuleType.getKeyProvider().apply(mockEntity);
    assertEquals(result, "urn:li:dataHubPageModule:test");
  }

  @Test
  public void testBatchLoadSuccess() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubPageModule:test-module-1";
    String urn2 = "urn:li:dataHubPageModule:test-module-2";
    List<String> urns = Arrays.asList(urn1, urn2);

    // Create mock entity responses
    EntityResponse response1 = createMockEntityResponse(urn1, "Test Module 1");
    EntityResponse response2 = createMockEntityResponse(urn2, "Test Module 2");

    Map<Urn, EntityResponse> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn(urn1), response1);
    entityMap.put(UrnUtils.getUrn(urn2), response2);

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubPageModule"), any(), eq(PageModuleType.ASPECTS_TO_FETCH)))
        .thenReturn(entityMap);

    // Execute batch load
    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<DataHubPageModule>> results =
        pageModuleType.batchLoad(urns, mockContext);

    // Verify results
    assertEquals(results.size(), 2);

    DataFetcherResult<DataHubPageModule> result1 = results.get(0);
    assertNotNull(result1);
    assertNotNull(result1.getData());
    assertEquals(result1.getData().getUrn(), urn1);
    assertEquals(result1.getData().getType(), EntityType.DATAHUB_PAGE_MODULE);
    assertEquals(result1.getData().getProperties().getName(), "Test Module 1");

    DataFetcherResult<DataHubPageModule> result2 = results.get(1);
    assertNotNull(result2);
    assertNotNull(result2.getData());
    assertEquals(result2.getData().getUrn(), urn2);
    assertEquals(result2.getData().getType(), EntityType.DATAHUB_PAGE_MODULE);
    assertEquals(result2.getData().getProperties().getName(), "Test Module 2");
  }

  @Test
  public void testBatchLoadWithMissingEntities() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubPageModule:test-module-1";
    String urn2 = "urn:li:dataHubPageModule:test-module-2";
    List<String> urns = Arrays.asList(urn1, urn2);

    // Create mock entity response for only one entity
    EntityResponse response1 = createMockEntityResponse(urn1, "Test Module 1");

    Map<Urn, EntityResponse> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn(urn1), response1);
    // Note: urn2 is missing from the map

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubPageModule"), any(), eq(PageModuleType.ASPECTS_TO_FETCH)))
        .thenReturn(entityMap);

    // Execute batch load
    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<DataHubPageModule>> results =
        pageModuleType.batchLoad(urns, mockContext);

    // Verify results
    assertEquals(results.size(), 2);

    DataFetcherResult<DataHubPageModule> result1 = results.get(0);
    assertNotNull(result1);
    assertNotNull(result1.getData());
    assertEquals(result1.getData().getUrn(), urn1);

    DataFetcherResult<DataHubPageModule> result2 = results.get(1);
    assertNull(result2); // Missing entity should return null
  }

  @Test
  public void testBatchLoadEmptyList() throws Exception {
    List<String> urns = Collections.emptyList();

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubPageModule"), any(), eq(PageModuleType.ASPECTS_TO_FETCH)))
        .thenReturn(Collections.emptyMap());

    // Execute batch load
    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<DataHubPageModule>> results =
        pageModuleType.batchLoad(urns, mockContext);

    // Verify results
    assertEquals(results.size(), 0);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testBatchLoadException() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubPageModule:test-module-1";
    List<String> urns = Arrays.asList(urn1);

    // Mock the entity client to throw an exception
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubPageModule"), any(), eq(PageModuleType.ASPECTS_TO_FETCH)))
        .thenThrow(new RemoteInvocationException("Test exception"));

    // Execute batch load - should throw RuntimeException
    QueryContext mockContext = getMockAllowContext();
    pageModuleType.batchLoad(urns, mockContext);
  }

  @Test
  public void testAspectsToFetch() {
    assertEquals(PageModuleType.ASPECTS_TO_FETCH.size(), 1);
    assertTrue(PageModuleType.ASPECTS_TO_FETCH.contains("dataHubPageModuleProperties"));
  }

  private EntityResponse createMockEntityResponse(String urn, String name) {
    // Create GMS properties
    DataHubPageModuleProperties gmsProperties = new DataHubPageModuleProperties();
    gmsProperties.setName(name);
    gmsProperties.setType(DataHubPageModuleType.LINK);

    // Create visibility
    DataHubPageModuleVisibility visibility = new DataHubPageModuleVisibility();
    visibility.setScope(PageModuleScope.GLOBAL);
    gmsProperties.setVisibility(visibility);

    // Create params with linkUrn
    com.linkedin.module.DataHubPageModuleParams params =
        new com.linkedin.module.DataHubPageModuleParams();
    com.linkedin.module.LinkModuleParams linkParams = new com.linkedin.module.LinkModuleParams();

    linkParams.setLinkUrl("https://example.com/test-link");

    params.setLinkParams(linkParams);
    gmsProperties.setParams(params);

    // Create audit stamps
    AuditStamp created = new AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    gmsProperties.setCreated(created);

    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(UrnUtils.getUrn("urn:li:corpuser:test-user"));
    gmsProperties.setLastModified(lastModified);

    // Create entity response
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn(urn));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(gmsProperties.data()));
    aspectMap.put("dataHubPageModuleProperties", aspect);
    entityResponse.setAspects(aspectMap);

    return entityResponse;
  }
}
