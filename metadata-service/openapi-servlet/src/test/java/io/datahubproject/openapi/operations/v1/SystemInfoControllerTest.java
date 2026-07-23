package io.datahubproject.openapi.operations.v1;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.system_info.ComponentInfo;
import com.linkedin.metadata.system_info.ComponentStatus;
import com.linkedin.metadata.system_info.SpringComponentsInfo;
import com.linkedin.metadata.system_info.SystemInfoException;
import com.linkedin.metadata.system_info.SystemInfoResponse;
import com.linkedin.metadata.system_info.SystemInfoService;
import com.linkedin.metadata.system_info.SystemPropertiesInfo;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import org.mockito.MockedStatic;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SystemInfoControllerTest {

  private SystemInfoController systemInfoController;
  private SystemInfoService mockSystemInfoService;
  private ObjectMapper mockObjectMapper;
  private ObjectWriter mockObjectWriter;
  private AuthorizerChain mockAuthorizerChain;
  private OperationContext mockSystemOperationContext;
  private HttpServletRequest mockRequest;

  @BeforeMethod
  public void setUp() {
    mockSystemInfoService = mock(SystemInfoService.class);
    mockObjectMapper = mock(ObjectMapper.class);
    mockObjectWriter = mock(ObjectWriter.class);
    mockAuthorizerChain = mock(AuthorizerChain.class);
    mockSystemOperationContext = mock(OperationContext.class);
    mockRequest = mock(HttpServletRequest.class);

    // Setup HttpServletRequest mocks for RequestContext.buildOpenapi()
    when(mockRequest.getRemoteAddr()).thenReturn("127.0.0.1");
    when(mockRequest.getHeader("User-Agent")).thenReturn("test-user-agent");
    when(mockRequest.getHeader("X-Forwarded-For")).thenReturn(null); // Use getRemoteAddr() fallback

    // Setup simple authentication - avoid complex static mocking for now
    Authentication authentication = mock(Authentication.class);
    Actor actor = mock(Actor.class);
    when(actor.toUrnStr()).thenReturn("urn:li:corpuser:testUser");
    when(authentication.getActor()).thenReturn(actor);
    AuthenticationContext.setAuthentication(authentication);

    systemInfoController =
        new SystemInfoController(
            mockSystemInfoService,
            mockObjectMapper,
            mockAuthorizerChain,
            mockSystemOperationContext);
  }

  @Test
  public void testGetSystemInfoSuccess() throws Exception {
    // Use try-with-resources to mock static dependencies
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // Given
      SystemInfoResponse systemInfo =
          SystemInfoResponse.builder()
              .springComponents(
                  SpringComponentsInfo.builder()
                      .gms(
                          ComponentInfo.builder()
                              .name("GMS")
                              .status(ComponentStatus.AVAILABLE)
                              .version("1.0.0")
                              .build())
                      .build())
              .build();

      String expectedJson =
          "{\n  \"springComponents\" : {\n    \"gms\" : {\n      \"name\" : \"GMS\"\n    }\n  }\n}";

      when(mockSystemInfoService.getSystemInfo()).thenReturn(systemInfo);
      when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
      when(mockObjectWriter.writeValueAsString(systemInfo)).thenReturn(expectedJson);

      // When
      ResponseEntity<String> response = systemInfoController.getSystemInfo(mockRequest);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.OK);
      assertEquals(response.getBody(), expectedJson);
      assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));

      verify(mockSystemInfoService).getSystemInfo();
      verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
      verify(mockObjectWriter).writeValueAsString(systemInfo);
    }
  }

  @Test
  public void testGetSystemInfoWithServiceException() {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // Given
      when(mockSystemInfoService.getSystemInfo())
          .thenThrow(new SystemInfoException("Service failure"));

      // When
      ResponseEntity<String> response = systemInfoController.getSystemInfo(mockRequest);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
      assertNull(response.getBody());

      verify(mockSystemInfoService).getSystemInfo();
      verifyNoInteractions(mockObjectMapper);
    }
  }

  @Test
  public void testGetSystemInfoWithJsonSerializationException() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // Given
      SystemInfoResponse systemInfo = SystemInfoResponse.builder().build();

      when(mockSystemInfoService.getSystemInfo()).thenReturn(systemInfo);
      when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
      when(mockObjectWriter.writeValueAsString(systemInfo))
          .thenThrow(new RuntimeException("JSON serialization failed"));

      // When
      ResponseEntity<String> response = systemInfoController.getSystemInfo(mockRequest);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
      assertNull(response.getBody());

      verify(mockSystemInfoService).getSystemInfo();
      verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
      verify(mockObjectWriter).writeValueAsString(systemInfo);
    }
  }

  @Test
  public void testGetSpringComponentsInfoSuccess() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // Given
      SpringComponentsInfo springComponentsInfo =
          SpringComponentsInfo.builder()
              .gms(
                  ComponentInfo.builder()
                      .name("GMS")
                      .status(ComponentStatus.AVAILABLE)
                      .version("1.0.0")
                      .build())
              .maeConsumer(
                  ComponentInfo.builder()
                      .name("MAE Consumer")
                      .status(ComponentStatus.AVAILABLE)
                      // TEMPORARY: Placeholder properties to avoid circular dependency
                      // This test will need to be updated when proper deployment mode detection is
                      // implemented
                      .properties(
                          Map.of(
                              "mode", "placeholder",
                              "deployment", "embedded",
                              "note",
                                  "Consumer info collection temporarily disabled to avoid circular dependency"))
                      .build())
              .build();

      String expectedJson = "{\n  \"gms\" : {\n    \"name\" : \"GMS\"\n  }\n}";

      when(mockSystemInfoService.getSpringComponentsInfo()).thenReturn(springComponentsInfo);
      when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
      when(mockObjectWriter.writeValueAsString(springComponentsInfo)).thenReturn(expectedJson);

      // When
      ResponseEntity<String> response = systemInfoController.getSpringComponentsInfo(mockRequest);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.OK);
      assertEquals(response.getBody(), expectedJson);
      assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));

      verify(mockSystemInfoService).getSpringComponentsInfo();
      verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
      verify(mockObjectWriter).writeValueAsString(springComponentsInfo);
    }
  }

  @Test
  public void testGetSpringComponentsInfoWithException() {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // Given
      when(mockSystemInfoService.getSpringComponentsInfo())
          .thenThrow(new RuntimeException("Spring components collection failed"));

      // When
      ResponseEntity<String> response = systemInfoController.getSpringComponentsInfo(mockRequest);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
      assertNull(response.getBody());

      verify(mockSystemInfoService).getSpringComponentsInfo();
      verifyNoInteractions(mockObjectMapper);
    }
  }

  @Test
  public void testGetSystemPropertiesInfoSuccess() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // Given
      SystemPropertiesInfo propertiesInfo =
          SystemPropertiesInfo.builder().totalProperties(100).redactedProperties(15).build();

      String expectedJson = "{\n  \"totalProperties\" : 100,\n  \"redactedProperties\" : 15\n}";

      when(mockSystemInfoService.getSystemPropertiesInfo()).thenReturn(propertiesInfo);
      when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
      when(mockObjectWriter.writeValueAsString(propertiesInfo)).thenReturn(expectedJson);

      // When
      ResponseEntity<String> response = systemInfoController.getSystemPropertiesInfo(mockRequest);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.OK);
      assertEquals(response.getBody(), expectedJson);
      assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));

      verify(mockSystemInfoService).getSystemPropertiesInfo();
      verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
      verify(mockObjectWriter).writeValueAsString(propertiesInfo);
    }
  }

  @Test
  public void testGetSystemPropertiesInfoWithException() {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // Given
      when(mockSystemInfoService.getSystemPropertiesInfo())
          .thenThrow(new RuntimeException("Properties collection failed"));

      // When
      ResponseEntity<String> response = systemInfoController.getSystemPropertiesInfo(mockRequest);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
      assertNull(response.getBody());

      verify(mockSystemInfoService).getSystemPropertiesInfo();
      verifyNoInteractions(mockObjectMapper);
    }
  }

  @Test
  public void testGetPropertiesAsMapSuccess() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // Given
      Map<String, Object> propertiesMap =
          Map.of(
              "app.name", "datahub",
              "server.port", "8080",
              "database.password", "***REDACTED***");

      String expectedJson = "{\n  \"app.name\" : \"datahub\",\n  \"server.port\" : \"8080\"\n}";

      when(mockSystemInfoService.getPropertiesAsMap()).thenReturn(propertiesMap);
      when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
      when(mockObjectWriter.writeValueAsString(propertiesMap)).thenReturn(expectedJson);

      // When
      ResponseEntity<String> response = systemInfoController.getPropertiesAsMap(mockRequest);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.OK);
      assertEquals(response.getBody(), expectedJson);
      assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));

      verify(mockSystemInfoService).getPropertiesAsMap();
      verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
      verify(mockObjectWriter).writeValueAsString(propertiesMap);
    }
  }

  @Test
  public void testGetPropertiesAsMapWithException() {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // Given
      when(mockSystemInfoService.getPropertiesAsMap())
          .thenThrow(new RuntimeException("Properties map collection failed"));

      // When
      ResponseEntity<String> response = systemInfoController.getPropertiesAsMap(mockRequest);

      // Then
      assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
      assertNull(response.getBody());

      verify(mockSystemInfoService).getPropertiesAsMap();
      verifyNoInteractions(mockObjectMapper);
    }
  }

  @Test
  public void testControllerProperlyFormatsJsonForAllEndpoints() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // This test verifies that all endpoints use pretty-printed JSON consistently
      // which is important for debugging and admin scenarios

      // Given - setup mock responses for all endpoints
      SystemInfoResponse systemInfo = SystemInfoResponse.builder().build();
      SpringComponentsInfo springComponents = SpringComponentsInfo.builder().build();
      SystemPropertiesInfo properties = SystemPropertiesInfo.builder().build();
      Map<String, Object> propertiesMap = Map.of();

      when(mockSystemInfoService.getSystemInfo()).thenReturn(systemInfo);
      when(mockSystemInfoService.getSpringComponentsInfo()).thenReturn(springComponents);
      when(mockSystemInfoService.getSystemPropertiesInfo()).thenReturn(properties);
      when(mockSystemInfoService.getPropertiesAsMap()).thenReturn(propertiesMap);

      // Setup the ObjectMapper -> ObjectWriter chain properly
      when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
      when(mockObjectWriter.writeValueAsString(any())).thenReturn("{}");

      // When - call all endpoints
      systemInfoController.getSystemInfo(mockRequest);
      systemInfoController.getSpringComponentsInfo(mockRequest);
      systemInfoController.getSystemPropertiesInfo(mockRequest);
      systemInfoController.getPropertiesAsMap(mockRequest);

      // Then - verify pretty printer was used for all endpoints
      verify(mockObjectMapper, times(4)).writerWithDefaultPrettyPrinter();
      verify(mockObjectWriter).writeValueAsString(systemInfo);
      verify(mockObjectWriter).writeValueAsString(springComponents);
      verify(mockObjectWriter).writeValueAsString(properties);
      verify(mockObjectWriter).writeValueAsString(propertiesMap);
    }
  }

  @Test
  public void testControllerReturnsCorrectContentType() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // This test verifies that all endpoints return the correct JSON content type

      // Given
      when(mockSystemInfoService.getSystemInfo()).thenReturn(SystemInfoResponse.builder().build());
      when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
      when(mockObjectWriter.writeValueAsString(any())).thenReturn("{}");

      // When
      ResponseEntity<String> response = systemInfoController.getSystemInfo(mockRequest);

      // Then
      assertNotNull(response.getHeaders().getContentType());
      assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));
    }
  }

  @Test
  public void testAllEndpointsHandleGenericExceptions() {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class);
        MockedStatic<OperationContext> opContextMock = mockStatic(OperationContext.class)) {

      // Setup mock OperationContext
      OperationContext mockOpContext = mock(OperationContext.class);
      opContextMock
          .when(() -> OperationContext.asSession(any(), any(), any(), any(), anyBoolean()))
          .thenReturn(mockOpContext);

      // Setup authorization to return true
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class),
                      eq(PoliciesConfig.MANAGE_SYSTEM_OPERATIONS_PRIVILEGE)))
          .thenReturn(true);

      // This test verifies that all endpoints properly handle unexpected exceptions
      // and return appropriate HTTP 500 responses

      // Given - setup generic exceptions for all service methods
      when(mockSystemInfoService.getSystemInfo())
          .thenThrow(new RuntimeException("Unexpected error"));
      when(mockSystemInfoService.getSpringComponentsInfo())
          .thenThrow(new RuntimeException("Unexpected error"));
      when(mockSystemInfoService.getSystemPropertiesInfo())
          .thenThrow(new RuntimeException("Unexpected error"));
      when(mockSystemInfoService.getPropertiesAsMap())
          .thenThrow(new RuntimeException("Unexpected error"));

      // When & Then - all endpoints should return 500
      assertEquals(
          systemInfoController.getSystemInfo(mockRequest).getStatusCode(),
          HttpStatus.INTERNAL_SERVER_ERROR);
      assertEquals(
          systemInfoController.getSpringComponentsInfo(mockRequest).getStatusCode(),
          HttpStatus.INTERNAL_SERVER_ERROR);
      assertEquals(
          systemInfoController.getSystemPropertiesInfo(mockRequest).getStatusCode(),
          HttpStatus.INTERNAL_SERVER_ERROR);
      assertEquals(
          systemInfoController.getPropertiesAsMap(mockRequest).getStatusCode(),
          HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
