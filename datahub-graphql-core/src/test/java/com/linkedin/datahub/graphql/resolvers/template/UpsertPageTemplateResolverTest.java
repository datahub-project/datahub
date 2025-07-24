package com.linkedin.datahub.graphql.resolvers.template;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.PageTemplateRowInput;
import com.linkedin.datahub.graphql.generated.PageTemplateScope;
import com.linkedin.datahub.graphql.generated.PageTemplateSurfaceType;
import com.linkedin.datahub.graphql.generated.UpsertPageTemplateInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.service.PageTemplateService;
import com.linkedin.template.DataHubPageTemplateProperties;
import com.linkedin.template.DataHubPageTemplateRowArray;
import com.linkedin.template.DataHubPageTemplateSurface;
import com.linkedin.template.DataHubPageTemplateVisibility;
import graphql.schema.DataFetchingEnvironment;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.testng.annotations.Test;

public class UpsertPageTemplateResolverTest {
  private static final String TEST_TEMPLATE_URN = "urn:li:dataHubPageTemplate:test";

  @Test
  public void testGetSuccess() throws Exception {
    PageTemplateService mockService = mock(PageTemplateService.class);
    UpsertPageTemplateResolver resolver = new UpsertPageTemplateResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    UpsertPageTemplateInput input = new UpsertPageTemplateInput();
    input.setUrn(TEST_TEMPLATE_URN);
    input.setRows(createTestRowInputs());
    input.setScope(PageTemplateScope.GLOBAL);
    input.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);

    Urn urn = UrnUtils.getUrn(TEST_TEMPLATE_URN);
    when(mockService.upsertPageTemplate(any(), eq(TEST_TEMPLATE_URN), any(), any(), any()))
        .thenReturn(urn);

    // Mock EntityResponse with a valid aspect map
    EntityResponse mockResponse = mock(EntityResponse.class);
    DataHubPageTemplateProperties properties = new DataHubPageTemplateProperties();

    // Set required fields
    DataHubPageTemplateRowArray rows = new DataHubPageTemplateRowArray();
    properties.setRows(rows);

    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(com.linkedin.template.PageTemplateSurfaceType.HOME_PAGE);
    properties.setSurface(surface);

    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(com.linkedin.template.PageTemplateScope.GLOBAL);
    properties.setVisibility(visibility);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:test"));
    properties.setCreated(auditStamp);
    properties.setLastModified(auditStamp);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(properties.data()));
    aspectMap.put(
        com.linkedin.metadata.Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME, aspect);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(mockResponse.getUrn()).thenReturn(urn);
    when(mockService.getPageTemplateEntityResponse(any(), eq(urn))).thenReturn(mockResponse);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();
    verify(mockService, times(1))
        .upsertPageTemplate(any(), eq(TEST_TEMPLATE_URN), any(), any(), any());
    verify(mockService, times(1)).getPageTemplateEntityResponse(any(), eq(urn));
  }

  @Test
  public void testGetSuccessWithGeneratedUrn() throws Exception {
    PageTemplateService mockService = mock(PageTemplateService.class);
    UpsertPageTemplateResolver resolver = new UpsertPageTemplateResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    UpsertPageTemplateInput input = new UpsertPageTemplateInput();
    input.setUrn(null); // null urn should generate a new one
    input.setRows(createTestRowInputs());
    input.setScope(PageTemplateScope.GLOBAL);
    input.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);

    Urn urn = UrnUtils.getUrn(TEST_TEMPLATE_URN);
    when(mockService.upsertPageTemplate(any(), eq(null), any(), any(), any())).thenReturn(urn);

    // Mock EntityResponse with a valid aspect map
    EntityResponse mockResponse = mock(EntityResponse.class);
    DataHubPageTemplateProperties properties = new DataHubPageTemplateProperties();

    // Set required fields
    DataHubPageTemplateRowArray rows = new DataHubPageTemplateRowArray();
    properties.setRows(rows);

    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(com.linkedin.template.PageTemplateSurfaceType.HOME_PAGE);
    properties.setSurface(surface);

    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(com.linkedin.template.PageTemplateScope.GLOBAL);
    properties.setVisibility(visibility);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:test"));
    properties.setCreated(auditStamp);
    properties.setLastModified(auditStamp);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(properties.data()));
    aspectMap.put(
        com.linkedin.metadata.Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME, aspect);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(mockResponse.getUrn()).thenReturn(urn);
    when(mockService.getPageTemplateEntityResponse(any(), eq(urn))).thenReturn(mockResponse);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();
    verify(mockService, times(1)).upsertPageTemplate(any(), eq(null), any(), any(), any());
    verify(mockService, times(1)).getPageTemplateEntityResponse(any(), eq(urn));
  }

  @Test
  public void testCreateGlobalTemplateSuccessWithPermission() throws Exception {
    PageTemplateService mockService = mock(PageTemplateService.class);
    UpsertPageTemplateResolver resolver = new UpsertPageTemplateResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    UpsertPageTemplateInput input = new UpsertPageTemplateInput();
    input.setUrn(null);
    input.setRows(createTestRowInputs());
    input.setScope(PageTemplateScope.GLOBAL);
    input.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);

    Urn urn = UrnUtils.getUrn(TEST_TEMPLATE_URN);
    when(mockService.upsertPageTemplate(any(), eq(null), any(), any(), any())).thenReturn(urn);

    // Mock EntityResponse with a valid aspect map
    EntityResponse mockResponse = mock(EntityResponse.class);
    DataHubPageTemplateProperties properties = new DataHubPageTemplateProperties();

    // Set required fields
    DataHubPageTemplateRowArray rows = new DataHubPageTemplateRowArray();
    properties.setRows(rows);

    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(com.linkedin.template.PageTemplateSurfaceType.HOME_PAGE);
    properties.setSurface(surface);

    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(com.linkedin.template.PageTemplateScope.GLOBAL);
    properties.setVisibility(visibility);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:test"));
    properties.setCreated(auditStamp);
    properties.setLastModified(auditStamp);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(properties.data()));
    aspectMap.put(
        com.linkedin.metadata.Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME, aspect);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(mockResponse.getUrn()).thenReturn(urn);
    when(mockService.getPageTemplateEntityResponse(any(), eq(urn))).thenReturn(mockResponse);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();
    verify(mockService, times(1)).upsertPageTemplate(any(), eq(null), any(), any(), any());
    verify(mockService, times(1)).getPageTemplateEntityResponse(any(), eq(urn));
  }

  @Test
  public void testCreateGlobalTemplateFailureWithoutPermission() throws Exception {
    PageTemplateService mockService = mock(PageTemplateService.class);
    UpsertPageTemplateResolver resolver = new UpsertPageTemplateResolver(mockService);

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    UpsertPageTemplateInput input = new UpsertPageTemplateInput();
    input.setUrn(null);
    input.setRows(createTestRowInputs());
    input.setScope(PageTemplateScope.GLOBAL);
    input.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);

    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testCreatePersonalTemplateSuccessWithoutPermission() throws Exception {
    PageTemplateService mockService = mock(PageTemplateService.class);
    UpsertPageTemplateResolver resolver = new UpsertPageTemplateResolver(mockService);

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    UpsertPageTemplateInput input = new UpsertPageTemplateInput();
    input.setUrn(null);
    input.setRows(createTestRowInputs());
    input.setScope(PageTemplateScope.PERSONAL);
    input.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);

    Urn urn = UrnUtils.getUrn(TEST_TEMPLATE_URN);
    when(mockService.upsertPageTemplate(any(), eq(null), any(), any(), any())).thenReturn(urn);

    // Mock EntityResponse with a valid aspect map
    EntityResponse mockResponse = mock(EntityResponse.class);
    DataHubPageTemplateProperties properties = new DataHubPageTemplateProperties();

    // Set required fields
    DataHubPageTemplateRowArray rows = new DataHubPageTemplateRowArray();
    properties.setRows(rows);

    DataHubPageTemplateSurface surface = new DataHubPageTemplateSurface();
    surface.setSurfaceType(com.linkedin.template.PageTemplateSurfaceType.HOME_PAGE);
    properties.setSurface(surface);

    DataHubPageTemplateVisibility visibility = new DataHubPageTemplateVisibility();
    visibility.setScope(com.linkedin.template.PageTemplateScope.PERSONAL);
    properties.setVisibility(visibility);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:test"));
    properties.setCreated(auditStamp);
    properties.setLastModified(auditStamp);

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new com.linkedin.entity.Aspect(properties.data()));
    aspectMap.put(
        com.linkedin.metadata.Constants.DATAHUB_PAGE_TEMPLATE_PROPERTIES_ASPECT_NAME, aspect);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(mockResponse.getUrn()).thenReturn(urn);
    when(mockService.getPageTemplateEntityResponse(any(), eq(urn))).thenReturn(mockResponse);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    resolver.get(mockEnv).join();
    verify(mockService, times(1)).upsertPageTemplate(any(), eq(null), any(), any(), any());
    verify(mockService, times(1)).getPageTemplateEntityResponse(any(), eq(urn));
  }

  @Test
  public void testGetThrowsException() throws Exception {
    PageTemplateService mockService = mock(PageTemplateService.class);
    UpsertPageTemplateResolver resolver = new UpsertPageTemplateResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    UpsertPageTemplateInput input = new UpsertPageTemplateInput();
    input.setUrn(TEST_TEMPLATE_URN);
    input.setRows(createTestRowInputs());
    input.setScope(PageTemplateScope.GLOBAL);
    input.setSurfaceType(PageTemplateSurfaceType.HOME_PAGE);

    when(mockService.upsertPageTemplate(any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("fail"));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verify(mockService, times(1)).upsertPageTemplate(any(), any(), any(), any(), any());
  }

  @Test
  public void testMapInputRows() throws Exception {
    PageTemplateService mockService = mock(PageTemplateService.class);
    UpsertPageTemplateResolver resolver = new UpsertPageTemplateResolver(mockService);
    // Use reflection to call private method
    PageTemplateRowInput rowInput = new PageTemplateRowInput();
    rowInput.setModules(
        Arrays.asList("urn:li:dataHubPageModule:module1", "urn:li:dataHubPageModule:module2"));
    List<PageTemplateRowInput> inputRows = Collections.singletonList(rowInput);
    java.lang.reflect.Method method =
        UpsertPageTemplateResolver.class.getDeclaredMethod("mapInputRows", List.class);
    method.setAccessible(true);
    Object result = method.invoke(resolver, inputRows);
    assertNotNull(result);
  }

  private List<PageTemplateRowInput> createTestRowInputs() {
    PageTemplateRowInput rowInput = new PageTemplateRowInput();
    rowInput.setModules(
        Arrays.asList("urn:li:dataHubPageModule:module1", "urn:li:dataHubPageModule:module2"));
    return Collections.singletonList(rowInput);
  }
}
