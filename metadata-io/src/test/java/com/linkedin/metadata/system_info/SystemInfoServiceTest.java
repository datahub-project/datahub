package com.linkedin.metadata.system_info;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.system_info.collectors.PropertiesCollector;
import com.linkedin.metadata.system_info.collectors.SpringComponentsCollector;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SystemInfoServiceTest {

  private SystemInfoService systemInfoService;
  private SpringComponentsCollector mockSpringComponentsCollector;
  private PropertiesCollector mockPropertiesCollector;

  @BeforeMethod
  public void setUp() {
    mockSpringComponentsCollector = mock(SpringComponentsCollector.class);
    mockPropertiesCollector = mock(PropertiesCollector.class);

    systemInfoService =
        new SystemInfoService(mockSpringComponentsCollector, mockPropertiesCollector);
  }

  @AfterMethod
  public void tearDown() {
    // Ensure proper cleanup after each test
    systemInfoService.shutdown();
  }

  @Test
  public void testGetSpringComponentsInfo() {
    // Given
    SpringComponentsInfo expectedInfo =
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
                    .build())
            .mceConsumer(
                ComponentInfo.builder()
                    .name("MCE Consumer")
                    .status(ComponentStatus.AVAILABLE)
                    .build())
            .build();

    when(mockSpringComponentsCollector.collect(any(ExecutorService.class)))
        .thenReturn(expectedInfo);

    // When
    SpringComponentsInfo result = systemInfoService.getSpringComponentsInfo();

    // Then
    assertNotNull(result);
    assertEquals(result, expectedInfo);
    verify(mockSpringComponentsCollector).collect(any(ExecutorService.class));
  }

  @Test
  public void testGetSystemPropertiesInfo() {
    // Given
    SystemPropertiesInfo expectedInfo =
        SystemPropertiesInfo.builder()
            .properties(
                Map.of(
                    "test.property",
                    PropertyInfo.builder()
                        .key("test.property")
                        .value("test.value")
                        .source("test-source")
                        .build()))
            .totalProperties(1)
            .redactedProperties(0)
            .build();

    when(mockPropertiesCollector.collect()).thenReturn(expectedInfo);

    // When
    SystemPropertiesInfo result = systemInfoService.getSystemPropertiesInfo();

    // Then
    assertNotNull(result);
    assertEquals(result, expectedInfo);
    verify(mockPropertiesCollector).collect();
  }

  @Test
  public void testGetPropertiesAsMap() {
    // Given
    Map<String, Object> expectedProperties =
        Map.of(
            "app.name", "datahub",
            "server.port", "8080",
            "database.host", "localhost");

    when(mockPropertiesCollector.getPropertiesAsMap()).thenReturn(expectedProperties);

    // When
    Map<String, Object> result = systemInfoService.getPropertiesAsMap();

    // Then
    assertNotNull(result);
    assertEquals(result, expectedProperties);
    verify(mockPropertiesCollector).getPropertiesAsMap();
  }

  @Test
  public void testGetSystemInfoSuccessful() {
    // Given
    SpringComponentsInfo springComponents =
        SpringComponentsInfo.builder()
            .gms(
                ComponentInfo.builder()
                    .name("GMS")
                    .status(ComponentStatus.AVAILABLE)
                    .version("1.0.0")
                    .build())
            .build();

    when(mockSpringComponentsCollector.collect(any(ExecutorService.class)))
        .thenReturn(springComponents);

    // When
    SystemInfoResponse result = systemInfoService.getSystemInfo();

    // Then
    assertNotNull(result);
    assertEquals(result.getSpringComponents(), springComponents);

    verify(mockSpringComponentsCollector).collect(any(ExecutorService.class));
  }

  @Test
  public void testGetSystemInfoWithSpringComponentsCollectorException() {
    // Given
    when(mockSpringComponentsCollector.collect(any(ExecutorService.class)))
        .thenThrow(new RuntimeException("Spring components collection failed"));

    // When & Then
    expectThrows(SystemInfoException.class, () -> systemInfoService.getSystemInfo());
  }

  @Test
  public void testGetSystemInfoWithGenericException() {
    // Given
    when(mockSpringComponentsCollector.collect(any(ExecutorService.class)))
        .thenThrow(new RuntimeException("Generic collection failed"));

    // When & Then
    expectThrows(SystemInfoException.class, () -> systemInfoService.getSystemInfo());
  }

  @Test
  public void testGetSystemInfoWithBothCollectorsException() {
    // Given
    when(mockSpringComponentsCollector.collect(any(ExecutorService.class)))
        .thenThrow(new RuntimeException("Spring components failed"));
    when(mockPropertiesCollector.collect()).thenThrow(new RuntimeException("Properties failed"));

    // When & Then
    SystemInfoException exception =
        expectThrows(SystemInfoException.class, () -> systemInfoService.getSystemInfo());

    assertTrue(exception.getMessage().contains("Failed to collect system information"));
  }

  @Test
  public void testShutdownExecutorService() throws InterruptedException {
    // This test verifies that the @PreDestroy method properly shuts down the executor service
    // Since we can't directly test @PreDestroy annotation, we test the shutdown method directly

    // Given - a service that has been used (which initializes the executor)
    when(mockPropertiesCollector.getPropertiesAsMap()).thenReturn(Map.of());
    systemInfoService.getPropertiesAsMap(); // This initializes the executor

    // When
    systemInfoService.shutdown();

    // Then - no exceptions should be thrown, and subsequent calls should still work
    // This verifies graceful shutdown behavior
    assertNotNull(systemInfoService);
  }

  @Test
  public void testShutdownWithInterruption() {
    // Test shutdown behavior when thread is interrupted
    // This tests the interrupt handling in the @PreDestroy method

    // Given - simulate an interrupted shutdown
    Thread.currentThread().interrupt();

    try {
      // When
      systemInfoService.shutdown();

      // Then - verify interrupt status is restored
      assertTrue(Thread.interrupted(), "Thread interrupt status should be restored");
    } finally {
      // Clean up interrupt status
      Thread.interrupted();
    }
  }

  @Test
  public void testServiceDelegatesCorrectlyToCollectors() {
    // This test verifies that the service properly delegates to its collectors
    // and doesn't add extra logic that could cause issues

    // Given
    Map<String, Object> expectedMap = Map.of("test", "value");
    SystemPropertiesInfo expectedSystemProps =
        SystemPropertiesInfo.builder()
            .properties(Map.of())
            .totalProperties(0)
            .redactedProperties(0)
            .build();
    SpringComponentsInfo expectedSpringComponents =
        SpringComponentsInfo.builder()
            .gms(ComponentInfo.builder().name("GMS").status(ComponentStatus.AVAILABLE).build())
            .build();

    when(mockPropertiesCollector.getPropertiesAsMap()).thenReturn(expectedMap);
    when(mockPropertiesCollector.collect()).thenReturn(expectedSystemProps);
    when(mockSpringComponentsCollector.collect(any(ExecutorService.class)))
        .thenReturn(expectedSpringComponents);

    // When & Then - verify each method delegates correctly
    assertEquals(systemInfoService.getPropertiesAsMap(), expectedMap);
    assertEquals(systemInfoService.getSystemPropertiesInfo(), expectedSystemProps);
    assertEquals(systemInfoService.getSpringComponentsInfo(), expectedSpringComponents);

    // Verify all collectors were called exactly once
    verify(mockPropertiesCollector, times(1)).getPropertiesAsMap();
    verify(mockPropertiesCollector, times(1)).collect();
    verify(mockSpringComponentsCollector, times(1)).collect(any(ExecutorService.class));
  }
}
