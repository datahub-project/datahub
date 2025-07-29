package io.datahubproject.openapi.operations.v1;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.linkedin.metadata.system_info.ComponentInfo;
import com.linkedin.metadata.system_info.ComponentStatus;
import com.linkedin.metadata.system_info.SpringComponentsInfo;
import com.linkedin.metadata.system_info.SystemInfoException;
import com.linkedin.metadata.system_info.SystemInfoResponse;
import com.linkedin.metadata.system_info.SystemInfoService;
import com.linkedin.metadata.system_info.SystemPropertiesInfo;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SystemInfoControllerTest {

  private SystemInfoController systemInfoController;
  private SystemInfoService mockSystemInfoService;
  private ObjectMapper mockObjectMapper;
  private ObjectWriter mockObjectWriter;

  @BeforeMethod
  public void setUp() {
    mockSystemInfoService = mock(SystemInfoService.class);
    mockObjectMapper = mock(ObjectMapper.class);
    mockObjectWriter = mock(ObjectWriter.class);

    systemInfoController = new SystemInfoController(mockSystemInfoService, mockObjectMapper);
  }

  @Test
  public void testGetSystemInfoSuccess() throws Exception {
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
    ResponseEntity<String> response = systemInfoController.getSystemInfo();

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertEquals(response.getBody(), expectedJson);
    assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));

    verify(mockSystemInfoService).getSystemInfo();
    verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
    verify(mockObjectWriter).writeValueAsString(systemInfo);
  }

  @Test
  public void testGetSystemInfoWithServiceException() {
    // Given
    when(mockSystemInfoService.getSystemInfo())
        .thenThrow(new SystemInfoException("Service failure"));

    // When
    ResponseEntity<String> response = systemInfoController.getSystemInfo();

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    assertNull(response.getBody());

    verify(mockSystemInfoService).getSystemInfo();
    verifyNoInteractions(mockObjectMapper);
  }

  @Test
  public void testGetSystemInfoWithJsonSerializationException() throws Exception {
    // Given
    SystemInfoResponse systemInfo = SystemInfoResponse.builder().build();

    when(mockSystemInfoService.getSystemInfo()).thenReturn(systemInfo);
    when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
    when(mockObjectWriter.writeValueAsString(systemInfo))
        .thenThrow(new RuntimeException("JSON serialization failed"));

    // When
    ResponseEntity<String> response = systemInfoController.getSystemInfo();

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    assertNull(response.getBody());

    verify(mockSystemInfoService).getSystemInfo();
    verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
    verify(mockObjectWriter).writeValueAsString(systemInfo);
  }

  @Test
  public void testGetSpringComponentsInfoSuccess() throws Exception {
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
    ResponseEntity<String> response = systemInfoController.getSpringComponentsInfo();

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertEquals(response.getBody(), expectedJson);
    assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));

    verify(mockSystemInfoService).getSpringComponentsInfo();
    verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
    verify(mockObjectWriter).writeValueAsString(springComponentsInfo);
  }

  @Test
  public void testGetSpringComponentsInfoWithException() {
    // Given
    when(mockSystemInfoService.getSpringComponentsInfo())
        .thenThrow(new RuntimeException("Spring components collection failed"));

    // When
    ResponseEntity<String> response = systemInfoController.getSpringComponentsInfo();

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    assertNull(response.getBody());

    verify(mockSystemInfoService).getSpringComponentsInfo();
    verifyNoInteractions(mockObjectMapper);
  }

  @Test
  public void testGetSystemPropertiesInfoSuccess() throws Exception {
    // Given
    SystemPropertiesInfo propertiesInfo =
        SystemPropertiesInfo.builder().totalProperties(100).redactedProperties(15).build();

    String expectedJson = "{\n  \"totalProperties\" : 100,\n  \"redactedProperties\" : 15\n}";

    when(mockSystemInfoService.getSystemPropertiesInfo()).thenReturn(propertiesInfo);
    when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
    when(mockObjectWriter.writeValueAsString(propertiesInfo)).thenReturn(expectedJson);

    // When
    ResponseEntity<String> response = systemInfoController.getSystemPropertiesInfo();

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertEquals(response.getBody(), expectedJson);
    assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));

    verify(mockSystemInfoService).getSystemPropertiesInfo();
    verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
    verify(mockObjectWriter).writeValueAsString(propertiesInfo);
  }

  @Test
  public void testGetSystemPropertiesInfoWithException() {
    // Given
    when(mockSystemInfoService.getSystemPropertiesInfo())
        .thenThrow(new RuntimeException("Properties collection failed"));

    // When
    ResponseEntity<String> response = systemInfoController.getSystemPropertiesInfo();

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    assertNull(response.getBody());

    verify(mockSystemInfoService).getSystemPropertiesInfo();
    verifyNoInteractions(mockObjectMapper);
  }

  @Test
  public void testGetPropertiesAsMapSuccess() throws Exception {
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
    ResponseEntity<String> response = systemInfoController.getPropertiesAsMap();

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertEquals(response.getBody(), expectedJson);
    assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));

    verify(mockSystemInfoService).getPropertiesAsMap();
    verify(mockObjectMapper).writerWithDefaultPrettyPrinter();
    verify(mockObjectWriter).writeValueAsString(propertiesMap);
  }

  @Test
  public void testGetPropertiesAsMapWithException() {
    // Given
    when(mockSystemInfoService.getPropertiesAsMap())
        .thenThrow(new RuntimeException("Properties map collection failed"));

    // When
    ResponseEntity<String> response = systemInfoController.getPropertiesAsMap();

    // Then
    assertEquals(response.getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    assertNull(response.getBody());

    verify(mockSystemInfoService).getPropertiesAsMap();
    verifyNoInteractions(mockObjectMapper);
  }

  @Test
  public void testControllerProperlyFormatsJsonForAllEndpoints() throws Exception {
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
    systemInfoController.getSystemInfo();
    systemInfoController.getSpringComponentsInfo();
    systemInfoController.getSystemPropertiesInfo();
    systemInfoController.getPropertiesAsMap();

    // Then - verify pretty printer was used for all endpoints
    verify(mockObjectMapper, times(4)).writerWithDefaultPrettyPrinter();
    verify(mockObjectWriter).writeValueAsString(systemInfo);
    verify(mockObjectWriter).writeValueAsString(springComponents);
    verify(mockObjectWriter).writeValueAsString(properties);
    verify(mockObjectWriter).writeValueAsString(propertiesMap);
  }

  @Test
  public void testControllerReturnsCorrectContentType() throws Exception {
    // This test verifies that all endpoints return the correct JSON content type

    // Given
    when(mockSystemInfoService.getSystemInfo()).thenReturn(SystemInfoResponse.builder().build());
    when(mockObjectMapper.writerWithDefaultPrettyPrinter()).thenReturn(mockObjectWriter);
    when(mockObjectWriter.writeValueAsString(any())).thenReturn("{}");

    // When
    ResponseEntity<String> response = systemInfoController.getSystemInfo();

    // Then
    assertNotNull(response.getHeaders().getContentType());
    assertTrue(response.getHeaders().getContentType().toString().contains("application/json"));
  }

  @Test
  public void testAllEndpointsHandleGenericExceptions() {
    // This test verifies that all endpoints properly handle unexpected exceptions
    // and return appropriate HTTP 500 responses

    // Given - setup generic exceptions for all service methods
    when(mockSystemInfoService.getSystemInfo()).thenThrow(new RuntimeException("Unexpected error"));
    when(mockSystemInfoService.getSpringComponentsInfo())
        .thenThrow(new RuntimeException("Unexpected error"));
    when(mockSystemInfoService.getSystemPropertiesInfo())
        .thenThrow(new RuntimeException("Unexpected error"));
    when(mockSystemInfoService.getPropertiesAsMap())
        .thenThrow(new RuntimeException("Unexpected error"));

    // When & Then - all endpoints should return 500
    assertEquals(
        systemInfoController.getSystemInfo().getStatusCode(), HttpStatus.INTERNAL_SERVER_ERROR);
    assertEquals(
        systemInfoController.getSpringComponentsInfo().getStatusCode(),
        HttpStatus.INTERNAL_SERVER_ERROR);
    assertEquals(
        systemInfoController.getSystemPropertiesInfo().getStatusCode(),
        HttpStatus.INTERNAL_SERVER_ERROR);
    assertEquals(
        systemInfoController.getPropertiesAsMap().getStatusCode(),
        HttpStatus.INTERNAL_SERVER_ERROR);
  }
}
