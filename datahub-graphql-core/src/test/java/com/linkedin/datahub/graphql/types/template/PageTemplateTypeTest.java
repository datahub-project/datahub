package com.linkedin.datahub.graphql.types.template;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.template.DataHubPageTemplateProperties;
import com.linkedin.template.DataHubPageTemplateRow;
import com.linkedin.template.DataHubPageTemplateRowArray;
import com.linkedin.template.DataHubPageTemplateSurface;
import com.linkedin.template.DataHubPageTemplateVisibility;
import com.linkedin.template.PageTemplateScope;
import com.linkedin.template.PageTemplateSurfaceType;
import graphql.execution.DataFetcherResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PageTemplateTypeTest {

  private PageTemplateType pageTemplateType;
  private EntityClient mockEntityClient;
  private QueryContext mockQueryContext;

  @BeforeMethod
  public void setUp() {
    mockEntityClient = mock(EntityClient.class);
    mockQueryContext = getMockAllowContext();
    pageTemplateType = new PageTemplateType(mockEntityClient);
  }

  @Test
  public void testType() {
    assertEquals(pageTemplateType.type(), EntityType.DATAHUB_PAGE_TEMPLATE);
  }

  @Test
  public void testObjectClass() {
    assertEquals(pageTemplateType.objectClass(), DataHubPageTemplate.class);
  }

  @Test
  public void testGetKeyProvider() {
    // Create a mock entity
    DataHubPageTemplate mockEntity = new DataHubPageTemplate();
    mockEntity.setUrn("urn:li:dataHubPageTemplate:test");

    String result = pageTemplateType.getKeyProvider().apply(mockEntity);
    assertEquals(result, "urn:li:dataHubPageTemplate:test");
  }

  @Test
  public void testBatchLoadSuccess() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubPageTemplate:test-template-1";
    String urn2 = "urn:li:dataHubPageTemplate:test-template-2";
    List<String> urns = Arrays.asList(urn1, urn2);

    // Create mock entity responses
    EntityResponse response1 = createMockEntityResponse(urn1, "Test Template 1");
    EntityResponse response2 = createMockEntityResponse(urn2, "Test Template 2");

    Map<Urn, EntityResponse> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn(urn1), response1);
    entityMap.put(UrnUtils.getUrn(urn2), response2);

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubPageTemplate"), any(), eq(PageTemplateType.ASPECTS_TO_FETCH)))
        .thenReturn(entityMap);

    // Execute batch load
    List<DataFetcherResult<DataHubPageTemplate>> results =
        pageTemplateType.batchLoad(urns, mockQueryContext);

    // Verify results
    assertEquals(results.size(), 2);

    DataFetcherResult<DataHubPageTemplate> result1 = results.get(0);
    assertNotNull(result1);
    assertNotNull(result1.getData());
    assertEquals(result1.getData().getUrn(), urn1);
    assertEquals(result1.getData().getType(), EntityType.DATAHUB_PAGE_TEMPLATE);
    assertEquals(result1.getData().getProperties().getRows().size(), 1);

    DataFetcherResult<DataHubPageTemplate> result2 = results.get(1);
    assertNotNull(result2);
    assertNotNull(result2.getData());
    assertEquals(result2.getData().getUrn(), urn2);
    assertEquals(result2.getData().getType(), EntityType.DATAHUB_PAGE_TEMPLATE);
    assertEquals(result2.getData().getProperties().getRows().size(), 1);
  }

  @Test
  public void testBatchLoadWithMissingEntities() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubPageTemplate:test-template-1";
    String urn2 = "urn:li:dataHubPageTemplate:test-template-2";
    List<String> urns = Arrays.asList(urn1, urn2);

    // Create mock entity response for only one entity
    EntityResponse response1 = createMockEntityResponse(urn1, "Test Template 1");

    Map<Urn, EntityResponse> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn(urn1), response1);
    // Note: urn2 is missing from the map

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubPageTemplate"), any(), eq(PageTemplateType.ASPECTS_TO_FETCH)))
        .thenReturn(entityMap);

    // Execute batch load
    List<DataFetcherResult<DataHubPageTemplate>> results =
        pageTemplateType.batchLoad(urns, mockQueryContext);

    // Verify results
    assertEquals(results.size(), 2);

    DataFetcherResult<DataHubPageTemplate> result1 = results.get(0);
    assertNotNull(result1);
    assertNotNull(result1.getData());
    assertEquals(result1.getData().getUrn(), urn1);

    DataFetcherResult<DataHubPageTemplate> result2 = results.get(1);
    assertNull(result2); // Missing entity should return null
  }

  @Test
  public void testBatchLoadEmptyList() throws Exception {
    List<String> urns = Collections.emptyList();

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubPageTemplate"), any(), eq(PageTemplateType.ASPECTS_TO_FETCH)))
        .thenReturn(Collections.emptyMap());

    // Execute batch load
    List<DataFetcherResult<DataHubPageTemplate>> results =
        pageTemplateType.batchLoad(urns, mockQueryContext);

    // Verify results
    assertEquals(results.size(), 0);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testBatchLoadException() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubPageTemplate:test-template-1";
    List<String> urns = Arrays.asList(urn1);

    // Mock the entity client to throw an exception
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubPageTemplate"), any(), eq(PageTemplateType.ASPECTS_TO_FETCH)))
        .thenThrow(new RemoteInvocationException("Test exception"));

    // Execute batch load - should throw RuntimeException
    pageTemplateType.batchLoad(urns, mockQueryContext);
  }

  @Test
  public void testAspectsToFetch() {
    assertEquals(PageTemplateType.ASPECTS_TO_FETCH.size(), 1);
    assertTrue(PageTemplateType.ASPECTS_TO_FETCH.contains("dataHubPageTemplateProperties"));
  }

  @Test
  public void testBatchLoadWithComplexTemplate() throws Exception {
    // Create test URNs
    String urn1 = "urn:li:dataHubPageTemplate:complex-template";
    List<String> urns = Arrays.asList(urn1);

    // Create mock entity response with complex template data
    EntityResponse response1 = createComplexMockEntityResponse(urn1);

    Map<Urn, EntityResponse> entityMap = new HashMap<>();
    entityMap.put(UrnUtils.getUrn(urn1), response1);

    // Mock the entity client response
    when(mockEntityClient.batchGetV2(
            any(), eq("dataHubPageTemplate"), any(), eq(PageTemplateType.ASPECTS_TO_FETCH)))
        .thenReturn(entityMap);

    // Execute batch load
    List<DataFetcherResult<DataHubPageTemplate>> results =
        pageTemplateType.batchLoad(urns, mockQueryContext);

    // Verify results
    assertEquals(results.size(), 1);

    DataFetcherResult<DataHubPageTemplate> result1 = results.get(0);
    assertNotNull(result1);
    assertNotNull(result1.getData());
    assertEquals(result1.getData().getUrn(), urn1);
    assertEquals(result1.getData().getType(), EntityType.DATAHUB_PAGE_TEMPLATE);

    // Verify complex properties
    assertNotNull(result1.getData().getProperties().getSurface());
    assertEquals(
        result1.getData().getProperties().getSurface().getSurfaceType(),
        com.linkedin.datahub.graphql.generated.PageTemplateSurfaceType.HOME_PAGE);

    assertNotNull(result1.getData().getProperties().getVisibility());
    assertEquals(
        result1.getData().getProperties().getVisibility().getScope(),
        com.linkedin.datahub.graphql.generated.PageTemplateScope.GLOBAL);

    assertNotNull(result1.getData().getProperties().getCreated());
    assertNotNull(result1.getData().getProperties().getLastModified());
  }

  private EntityResponse createMockEntityResponse(String urn, String name) {
    // Create GMS properties
    DataHubPageTemplateProperties gmsProperties = new DataHubPageTemplateProperties();

    // Create rows with modules
    DataHubPageTemplateRow row = new DataHubPageTemplateRow();
    Urn moduleUrn = UrnUtils.getUrn("urn:li:dataHubPageModule:test-module");
    UrnArray moduleUrns = new UrnArray();
    moduleUrns.add(moduleUrn);
    row.setModules(moduleUrns);

    DataHubPageTemplateRowArray rows = new DataHubPageTemplateRowArray();
    rows.add(row);
    gmsProperties.setRows(rows);

    // Create surface
    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);
    gmsProperties.setSurface(surface);

    // Create visibility
    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(PageTemplateScope.GLOBAL);
    gmsProperties.setVisibility(visibility);

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
    aspectMap.put("dataHubPageTemplateProperties", aspect);
    entityResponse.setAspects(aspectMap);

    return entityResponse;
  }

  private EntityResponse createComplexMockEntityResponse(String urn) {
    // Create GMS properties with more complex data
    DataHubPageTemplateProperties gmsProperties = new DataHubPageTemplateProperties();

    // Create multiple rows with multiple modules
    DataHubPageTemplateRowArray rows = new DataHubPageTemplateRowArray();

    // First row with one module
    DataHubPageTemplateRow row1 = new DataHubPageTemplateRow();
    UrnArray moduleUrns1 = new UrnArray();
    moduleUrns1.add(UrnUtils.getUrn("urn:li:dataHubPageModule:module-1"));
    row1.setModules(moduleUrns1);
    rows.add(row1);

    // Second row with two modules
    DataHubPageTemplateRow row2 = new DataHubPageTemplateRow();
    UrnArray moduleUrns2 = new UrnArray();
    moduleUrns2.add(UrnUtils.getUrn("urn:li:dataHubPageModule:module-2"));
    moduleUrns2.add(UrnUtils.getUrn("urn:li:dataHubPageModule:module-3"));
    row2.setModules(moduleUrns2);
    rows.add(row2);

    gmsProperties.setRows(rows);

    // Create surface
    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);
    gmsProperties.setSurface(surface);

    // Create visibility
    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(PageTemplateScope.GLOBAL);
    gmsProperties.setVisibility(visibility);

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
    aspectMap.put("dataHubPageTemplateProperties", aspect);
    entityResponse.setAspects(aspectMap);

    return entityResponse;
  }
}
