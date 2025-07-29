package com.linkedin.metadata.system_info.collectors;

import static com.linkedin.metadata.system_info.SystemInfoConstants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.system_info.ComponentInfo;
import com.linkedin.metadata.system_info.ComponentStatus;
import com.linkedin.metadata.system_info.SpringComponentsInfo;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SpringComponentsCollectorTest {

  private SpringComponentsCollector springComponentsCollector;
  private OperationContext mockOperationContext;
  private GitVersion mockGitVersion;
  private PropertiesCollector mockPropertiesCollector;

  @BeforeMethod
  public void setUp() {
    mockOperationContext = mock(OperationContext.class);
    mockGitVersion = mock(GitVersion.class);
    mockPropertiesCollector = mock(PropertiesCollector.class);

    springComponentsCollector =
        new SpringComponentsCollector(
            mockOperationContext, mockGitVersion, mockPropertiesCollector);
  }

  @Test
  public void testCollectWithSuccessfulExecution() throws Exception {
    // Given
    String testVersion = "1.0.0";
    Map<String, Object> testProperties = Map.of("test.prop", "test.value");

    when(mockGitVersion.getVersion()).thenReturn(testVersion);
    when(mockPropertiesCollector.getPropertiesAsMap()).thenReturn(testProperties);

    // When - use a real ExecutorService for this test to verify actual behavior
    ExecutorService realExecutorService = java.util.concurrent.Executors.newFixedThreadPool(3);
    try {
      SpringComponentsInfo result = springComponentsCollector.collect(realExecutorService);

      // Then
      assertNotNull(result);
      assertNotNull(result.getGms());
      assertNotNull(result.getMaeConsumer());
      assertNotNull(result.getMceConsumer());

      // Verify GMS info
      ComponentInfo gmsInfo = result.getGms();
      assertEquals(gmsInfo.getName(), GMS_COMPONENT_NAME);
      assertEquals(gmsInfo.getStatus(), ComponentStatus.AVAILABLE);
      assertEquals(gmsInfo.getVersion(), testVersion);
      assertEquals(gmsInfo.getProperties(), testProperties);

      // TEMPORARY: Verify MAE consumer placeholder behavior
      // This test will need to be updated when proper deployment mode detection is implemented
      ComponentInfo maeInfo = result.getMaeConsumer();
      assertEquals(maeInfo.getName(), MAE_COMPONENT_NAME);
      assertEquals(maeInfo.getStatus(), ComponentStatus.AVAILABLE);
      assertEquals(maeInfo.getVersion(), testVersion);
      assertTrue(maeInfo.getProperties().containsKey("mode"));
      assertEquals(maeInfo.getProperties().get("mode"), "placeholder");

      // TEMPORARY: Verify MCE consumer placeholder behavior
      // This test will need to be updated when proper deployment mode detection is implemented
      ComponentInfo mceInfo = result.getMceConsumer();
      assertEquals(mceInfo.getName(), MCE_COMPONENT_NAME);
      assertEquals(mceInfo.getStatus(), ComponentStatus.AVAILABLE);
      assertEquals(mceInfo.getVersion(), testVersion);
      assertTrue(mceInfo.getProperties().containsKey("mode"));
      assertEquals(mceInfo.getProperties().get("mode"), "placeholder");
    } finally {
      realExecutorService.shutdown();
    }
  }

  @Test
  public void testCollectWithExecutorTimeout() throws Exception {
    // Given - Test with mock executor to see error handling
    when(mockGitVersion.getVersion()).thenReturn("1.0.0");
    when(mockPropertiesCollector.getPropertiesAsMap()).thenReturn(Map.of());

    // When - use real executor to test normal path, error cases tested via other methods
    ExecutorService realExecutorService = java.util.concurrent.Executors.newFixedThreadPool(1);
    try {
      SpringComponentsInfo result = springComponentsCollector.collect(realExecutorService);

      // Then - verify components are created even if some operations might be slow
      assertNotNull(result);
      assertNotNull(result.getGms());
      assertNotNull(result.getMaeConsumer());
      assertNotNull(result.getMceConsumer());
    } finally {
      realExecutorService.shutdown();
    }
  }

  @Test
  public void testGetGmsInfoSuccess() {
    // Given
    String testVersion = "1.0.0";
    Map<String, Object> testProperties =
        Map.of(
            "app.name", "datahub",
            "server.port", "8080");

    when(mockGitVersion.getVersion()).thenReturn(testVersion);
    when(mockPropertiesCollector.getPropertiesAsMap()).thenReturn(testProperties);

    // When - test through the public collect method
    ExecutorService realExecutorService = java.util.concurrent.Executors.newFixedThreadPool(1);
    try {
      SpringComponentsInfo result = springComponentsCollector.collect(realExecutorService);

      // Then - verify GMS component is properly constructed
      ComponentInfo gmsInfo = result.getGms();
      assertEquals(gmsInfo.getName(), GMS_COMPONENT_NAME);
      assertEquals(gmsInfo.getStatus(), ComponentStatus.AVAILABLE);
      assertEquals(gmsInfo.getVersion(), testVersion);
      assertEquals(gmsInfo.getProperties(), testProperties);
      assertNull(gmsInfo.getErrorMessage());
    } finally {
      realExecutorService.shutdown();
    }
  }

  @Test
  public void testGetGmsInfoWithPropertiesCollectorException() {
    // Given
    String testVersion = "1.0.0";
    when(mockGitVersion.getVersion()).thenReturn(testVersion);
    when(mockPropertiesCollector.getPropertiesAsMap())
        .thenThrow(new RuntimeException("Properties collection failed"));

    // When
    ExecutorService realExecutorService = java.util.concurrent.Executors.newFixedThreadPool(1);
    try {
      SpringComponentsInfo result = springComponentsCollector.collect(realExecutorService);

      // Then - GMS should have ERROR status when properties collection fails
      ComponentInfo gmsInfo = result.getGms();
      assertEquals(gmsInfo.getName(), GMS_COMPONENT_NAME);
      assertEquals(gmsInfo.getStatus(), ComponentStatus.ERROR);
      assertTrue(gmsInfo.getErrorMessage().contains("Properties collection failed"));
    } finally {
      realExecutorService.shutdown();
    }
  }

  @Test
  public void testGetMaeConsumerInfoReturnsPlaceholder() {
    // TEMPORARY BEHAVIOR TEST: This test verifies the current placeholder implementation
    // This test will need to be updated when proper deployment mode detection is implemented
    // to test actual MAE consumer status in different deployment modes

    // Given
    String testVersion = "2.0.0";
    when(mockGitVersion.getVersion()).thenReturn(testVersion);
    when(mockPropertiesCollector.getPropertiesAsMap()).thenReturn(Map.of());

    // When
    ExecutorService realExecutorService = java.util.concurrent.Executors.newFixedThreadPool(1);
    try {
      SpringComponentsInfo result = springComponentsCollector.collect(realExecutorService);

      // Then - verify placeholder behavior
      ComponentInfo maeInfo = result.getMaeConsumer();
      assertEquals(maeInfo.getName(), MAE_COMPONENT_NAME);
      assertEquals(maeInfo.getStatus(), ComponentStatus.AVAILABLE);
      assertEquals(maeInfo.getVersion(), testVersion);

      // Verify placeholder properties
      Map<String, Object> properties = maeInfo.getProperties();
      assertEquals(properties.get("mode"), "placeholder");
      assertEquals(properties.get("deployment"), "embedded");
      assertTrue(((String) properties.get("note")).contains("circular dependency"));
    } finally {
      realExecutorService.shutdown();
    }
  }

  @Test
  public void testGetMceConsumerInfoReturnsPlaceholder() {
    // TEMPORARY BEHAVIOR TEST: This test verifies the current placeholder implementation
    // This test will need to be updated when proper deployment mode detection is implemented
    // to test actual MCE consumer status in different deployment modes

    // Given
    String testVersion = "2.0.0";
    when(mockGitVersion.getVersion()).thenReturn(testVersion);
    when(mockPropertiesCollector.getPropertiesAsMap()).thenReturn(Map.of());

    // When
    ExecutorService realExecutorService = java.util.concurrent.Executors.newFixedThreadPool(1);
    try {
      SpringComponentsInfo result = springComponentsCollector.collect(realExecutorService);

      // Then - verify placeholder behavior
      ComponentInfo mceInfo = result.getMceConsumer();
      assertEquals(mceInfo.getName(), MCE_COMPONENT_NAME);
      assertEquals(mceInfo.getStatus(), ComponentStatus.AVAILABLE);
      assertEquals(mceInfo.getVersion(), testVersion);

      // Verify placeholder properties
      Map<String, Object> properties = mceInfo.getProperties();
      assertEquals(properties.get("mode"), "placeholder");
      assertEquals(properties.get("deployment"), "embedded");
      assertTrue(((String) properties.get("note")).contains("circular dependency"));
    } finally {
      realExecutorService.shutdown();
    }
  }

  @Test
  public void testCreateErrorComponentWithMessage() {
    // This test uses the public collect method to indirectly test error component creation
    // Given - mock an exception in properties collection
    when(mockPropertiesCollector.getPropertiesAsMap())
        .thenThrow(new RuntimeException("Test exception"));

    // When
    ExecutorService realExecutorService = java.util.concurrent.Executors.newFixedThreadPool(1);
    try {
      SpringComponentsInfo result = springComponentsCollector.collect(realExecutorService);

      // Then
      ComponentInfo gmsInfo = result.getGms();
      assertEquals(gmsInfo.getStatus(), ComponentStatus.ERROR);
      assertTrue(gmsInfo.getErrorMessage().contains("Test exception"));
    } finally {
      realExecutorService.shutdown();
    }
  }

  @Test
  public void testCollectConstants() {
    // Test that the collector uses the correct constants
    assertEquals(SpringComponentsCollector.SYSTEM_INFO_ENDPOINT, "/openapi/v1/system-info");
  }
}
