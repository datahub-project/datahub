package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataHubPageTemplateKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.template.DataHubPageTemplateProperties;
import com.linkedin.template.DataHubPageTemplateRow;
import com.linkedin.template.PageTemplateScope;
import com.linkedin.template.PageTemplateSurfaceType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PageTemplateServiceTest {
  private EntityClient mockEntityClient;
  private PageTemplateService service;
  private OperationContext mockOpContext;
  private Urn templateUrn;
  private DataHubPageTemplateKey key;

  @BeforeMethod
  public void setup() throws Exception {
    mockEntityClient = mock(EntityClient.class);
    service = new PageTemplateService(mockEntityClient);
    mockOpContext = mock(OperationContext.class);
    key = new DataHubPageTemplateKey().setId("test-id");
    templateUrn =
        EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATAHUB_PAGE_TEMPLATE_ENTITY_NAME);
    when(mockOpContext.getAuditStamp()).thenReturn(new AuditStamp());
  }

  @Test
  public void testUpsertPageTemplateSuccessWithUrn() throws Exception {
    when(mockEntityClient.batchIngestProposals(any(), anyList(), eq(false))).thenReturn(null);
    when(mockEntityClient.exists(any(), any())).thenReturn(true);

    List<DataHubPageTemplateRow> rows = createTestRows();
    Urn urn =
        service.upsertPageTemplate(
            mockOpContext,
            templateUrn.toString(),
            rows,
            PageTemplateScope.GLOBAL,
            PageTemplateSurfaceType.HOME_PAGE);
    assertNotNull(urn);
    assertEquals(urn.toString(), templateUrn.toString());
    verify(mockEntityClient, times(1)).batchIngestProposals(any(), anyList(), eq(false));
    verify(mockEntityClient, times(2)).exists(any(), any()); // 2 modules in test rows
  }

  @Test
  public void testUpsertPageTemplateSuccessWithGeneratedUrn() throws Exception {
    when(mockEntityClient.batchIngestProposals(any(), anyList(), eq(false))).thenReturn(null);
    when(mockEntityClient.exists(any(), any())).thenReturn(true);

    List<DataHubPageTemplateRow> rows = createTestRows();
    Urn urn =
        service.upsertPageTemplate(
            mockOpContext,
            null, // null urn should generate a new one
            rows,
            PageTemplateScope.GLOBAL,
            PageTemplateSurfaceType.HOME_PAGE);
    assertNotNull(urn);
    assertTrue(urn.toString().startsWith("urn:li:dataHubPageTemplate:"));
    verify(mockEntityClient, times(1)).batchIngestProposals(any(), anyList(), eq(false));
    verify(mockEntityClient, times(2)).exists(any(), any()); // 2 modules in test rows
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testUpsertPageTemplateFailure() throws Exception {
    doThrow(new RuntimeException("fail"))
        .when(mockEntityClient)
        .batchIngestProposals(any(), anyList(), eq(false));
    when(mockEntityClient.exists(any(), any())).thenReturn(true);

    service.upsertPageTemplate(
        mockOpContext,
        templateUrn.toString(),
        Collections.emptyList(),
        PageTemplateScope.GLOBAL,
        PageTemplateSurfaceType.HOME_PAGE);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testUpsertPageTemplateWithNonExistentModule() throws Exception {
    when(mockEntityClient.exists(any(), any())).thenReturn(false);

    List<DataHubPageTemplateRow> rows = createTestRows();
    service.upsertPageTemplate(
        mockOpContext,
        templateUrn.toString(),
        rows,
        PageTemplateScope.GLOBAL,
        PageTemplateSurfaceType.HOME_PAGE);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testUpsertPageTemplateWithModuleValidationException() throws Exception {
    when(mockEntityClient.exists(any(), any()))
        .thenThrow(new RuntimeException("Validation failed"));

    List<DataHubPageTemplateRow> rows = createTestRows();
    service.upsertPageTemplate(
        mockOpContext,
        templateUrn.toString(),
        rows,
        PageTemplateScope.GLOBAL,
        PageTemplateSurfaceType.HOME_PAGE);
  }

  @Test
  public void testGetPageTemplatePropertiesFound() throws Exception {
    DataHubPageTemplateProperties properties = new DataHubPageTemplateProperties();
    EntityResponse response = mock(EntityResponse.class);
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(properties.data()));
    aspectMap.put(Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME, aspect);
    when(response.getAspects()).thenReturn(aspectMap);
    when(mockEntityClient.getV2(any(), anyString(), any(), any(), eq(false))).thenReturn(response);
    DataHubPageTemplateProperties result =
        service.getPageTemplateProperties(mockOpContext, templateUrn);
    assertNotNull(result);
  }

  @Test
  public void testGetPageTemplatePropertiesNotFound() throws Exception {
    EntityResponse response = mock(EntityResponse.class);
    EnvelopedAspectMap emptyAspectMap = new EnvelopedAspectMap();
    when(response.getAspects()).thenReturn(emptyAspectMap);
    when(mockEntityClient.getV2(any(), anyString(), any(), any(), eq(false))).thenReturn(response);
    DataHubPageTemplateProperties result =
        service.getPageTemplateProperties(mockOpContext, templateUrn);
    assertNull(result);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testGetPageTemplateEntityResponseThrows() throws Exception {
    when(mockEntityClient.getV2(any(), anyString(), any(), any(), eq(false)))
        .thenThrow(new RuntimeException("fail"));
    service.getPageTemplateEntityResponse(mockOpContext, templateUrn);
  }

  private List<DataHubPageTemplateRow> createTestRows() {
    DataHubPageTemplateRow row = new DataHubPageTemplateRow();
    row.setModules(
        new UrnArray(
            Arrays.asList(
                UrnUtils.getUrn("urn:li:dataHubPageModule:module1"),
                UrnUtils.getUrn("urn:li:dataHubPageModule:module2"))));
    return Collections.singletonList(row);
  }
}
