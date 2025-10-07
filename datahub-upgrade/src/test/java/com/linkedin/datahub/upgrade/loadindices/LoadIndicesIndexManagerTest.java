package com.linkedin.datahub.upgrade.loadindices;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadIndicesIndexManagerTest {

  private LoadIndicesIndexManager indexManager;
  private SearchClientShim<?> mockSearchClient;
  private IndexConvention mockIndexConvention;
  private EntityRegistry mockEntityRegistry;
  private String configuredRefreshInterval;

  @BeforeMethod
  public void setUp() {
    mockSearchClient = mock(SearchClientShim.class);
    mockIndexConvention = mock(IndexConvention.class);
    mockEntityRegistry = mock(EntityRegistry.class);
    configuredRefreshInterval = "3s";

    indexManager =
        new LoadIndicesIndexManager(
            mockSearchClient, mockIndexConvention, mockEntityRegistry, configuredRefreshInterval);
  }

  @Test
  public void testConstructor() {
    assertNotNull(indexManager);
    assertFalse(indexManager.isRefreshDisabled());
  }

  @Test
  public void testDiscoverDataHubIndices() throws IOException {
    // Mock the GetIndexResponse
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {
      "datahub_dataset_v2",
      "datahub_dashboard_v2",
      "system_index",
      "other_index",
      "datahub_chart_v2"
    };
    when(mockResponse.getIndices()).thenReturn(allIndices);

    // Mock the search client
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    entitySpecs.put("dashboard", mock(com.linkedin.metadata.models.EntitySpec.class));
    entitySpecs.put("chart", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    // Mock index convention to determine DataHub indices
    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
    when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");
    when(mockIndexConvention.getEntityIndexName("chart")).thenReturn("datahub_chart_v2");

    Set<String> result = indexManager.discoverDataHubIndices();

    assertNotNull(result);
    assertEquals(result.size(), 3);
    assertTrue(result.contains("datahub_dataset_v2"));
    assertTrue(result.contains("datahub_dashboard_v2"));
    assertTrue(result.contains("datahub_chart_v2"));
    assertFalse(result.contains("system_index"));
    assertFalse(result.contains("other_index"));
  }

  @Test
  public void testDiscoverDataHubIndicesWithIOException() throws IOException {
    // Mock IOException
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Connection failed"));

    assertThrows(IOException.class, () -> indexManager.discoverDataHubIndices());
  }

  @Test
  public void testDisableRefresh() throws IOException {
    // Mock discoverDataHubIndices behavior
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");

    // Mock settings response
    GetSettingsResponse mockSettingsResponse = mock(GetSettingsResponse.class);
    Settings mockSettings = mock(Settings.class);
    when(mockSettings.get("index.refresh_interval")).thenReturn("1s");
    when(mockSettingsResponse.getIndexToSettings())
        .thenReturn(java.util.Collections.singletonMap("datahub_dataset_v2", mockSettings));

    when(mockSearchClient.getIndexSettings(
            any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSettingsResponse);

    // Mock update settings
    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null);

    indexManager.disableRefresh();

    assertTrue(indexManager.isRefreshDisabled());
  }

  @Test
  public void testDisableRefreshWithIOException() throws IOException {
    // Mock IOException during discovery
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Discovery failed"));

    assertThrows(IOException.class, () -> indexManager.disableRefresh());
  }

  @Test
  public void testRestoreRefresh() throws IOException {
    // First disable refresh to set up state
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");

    GetSettingsResponse mockSettingsResponse = mock(GetSettingsResponse.class);
    Settings mockSettings = mock(Settings.class);
    when(mockSettings.get("index.refresh_interval")).thenReturn("1s");
    when(mockSettingsResponse.getIndexToSettings())
        .thenReturn(java.util.Collections.singletonMap("datahub_dataset_v2", mockSettings));

    when(mockSearchClient.getIndexSettings(
            any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSettingsResponse);

    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null);

    // Disable refresh first
    indexManager.disableRefresh();
    assertTrue(indexManager.isRefreshDisabled());

    // Now test restore
    indexManager.restoreRefresh();

    assertFalse(indexManager.isRefreshDisabled());
  }

  @Test
  public void testRestoreRefreshWithIOException() throws IOException {
    // Set up state first
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");

    GetSettingsResponse mockSettingsResponse = mock(GetSettingsResponse.class);
    Settings mockSettings = mock(Settings.class);
    when(mockSettings.get("index.refresh_interval")).thenReturn("1s");
    when(mockSettingsResponse.getIndexToSettings())
        .thenReturn(java.util.Collections.singletonMap("datahub_dataset_v2", mockSettings));

    when(mockSearchClient.getIndexSettings(
            any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSettingsResponse);

    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null);

    // Disable refresh first
    indexManager.disableRefresh();
    assertTrue(indexManager.isRefreshDisabled());

    // Mock IOException during restore
    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Restore failed"));

    // The implementation throws IOException when restore fails
    assertThrows(IOException.class, () -> indexManager.restoreRefresh());
  }

  @Test
  public void testRestoreRefreshWhenNotDisabled() throws IOException {
    // Should not throw exception when refresh is not disabled
    indexManager.restoreRefresh();
    assertFalse(indexManager.isRefreshDisabled());
  }

  @Test
  public void testIsDataHubIndex() {
    // Test with reflection to access private method
    try {
      var method = LoadIndicesIndexManager.class.getDeclaredMethod("isDataHubIndex", String.class);
      method.setAccessible(true);

      // Mock EntityRegistry to return entity specs
      Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
      entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
      entitySpecs.put("dashboard", mock(com.linkedin.metadata.models.EntitySpec.class));
      when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

      // Mock index convention
      when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
      when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");

      // Test DataHub indices
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dataset_v2"));
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dashboard_v2"));

      // Test non-DataHub indices
      assertFalse((Boolean) method.invoke(indexManager, "system_index"));
      assertFalse((Boolean) method.invoke(indexManager, "other_index"));
      assertFalse((Boolean) method.invoke(indexManager, "random_index"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndex via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testGetManagedIndices() {
    // Test with reflection to access private field
    try {
      var field = LoadIndicesIndexManager.class.getDeclaredField("managedIndices");
      field.setAccessible(true);

      Set<String> result = (Set<String>) field.get(indexManager);
      assertNotNull(result);
      assertTrue(result.isEmpty()); // Initially empty

    } catch (Exception e) {
      fail("Failed to test managedIndices field via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testMultipleDisableRefreshCalls() throws IOException {
    // Mock responses
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");

    GetSettingsResponse mockSettingsResponse = mock(GetSettingsResponse.class);
    Settings mockSettings = mock(Settings.class);
    when(mockSettings.get("index.refresh_interval")).thenReturn("1s");
    when(mockSettingsResponse.getIndexToSettings())
        .thenReturn(java.util.Collections.singletonMap("datahub_dataset_v2", mockSettings));

    when(mockSearchClient.getIndexSettings(
            any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSettingsResponse);

    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null);

    // First call
    indexManager.disableRefresh();
    assertTrue(indexManager.isRefreshDisabled());

    // Second call should not discover indices again
    indexManager.disableRefresh();
    assertTrue(indexManager.isRefreshDisabled());

    // Verify discovery was called only once
    verify(mockSearchClient, times(1))
        .getIndex(any(GetIndexRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testEmptyIndicesDiscovery() throws IOException {
    // Mock empty response
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    when(mockResponse.getIndices()).thenReturn(new String[0]);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    Set<String> result = indexManager.discoverDataHubIndices();

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testDisableRefreshWithIOExceptionOnFirstIndex() throws IOException {
    // Mock responses for index discovery
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2", "datahub_dashboard_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    entitySpecs.put("dashboard", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
    when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");

    // Mock IOException during first index refresh disable
    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Failed to disable refresh for first index"));

    // Should throw IOException when first index fails
    assertThrows(IOException.class, () -> indexManager.disableRefresh());

    // Verify refresh is still disabled = false since operation failed
    assertFalse(indexManager.isRefreshDisabled());
  }

  @Test
  public void testDisableRefreshWithIOExceptionOnSecondIndex() throws IOException {
    // Mock responses for index discovery
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2", "datahub_dashboard_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    entitySpecs.put("dashboard", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
    when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");

    // First call succeeds, second call throws IOException
    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null) // First index succeeds
        .thenThrow(
            new IOException("Failed to disable refresh for second index")); // Second index fails

    // Should throw IOException when second index fails
    assertThrows(IOException.class, () -> indexManager.disableRefresh());

    // Verify refresh is still disabled = false since operation failed
    assertFalse(indexManager.isRefreshDisabled());
  }

  @Test
  public void testDisableRefreshWithIOExceptionOnLastIndex() throws IOException {
    // Mock responses for index discovery
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2", "datahub_dashboard_v2", "datahub_chart_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    entitySpecs.put("dashboard", mock(com.linkedin.metadata.models.EntitySpec.class));
    entitySpecs.put("chart", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
    when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");
    when(mockIndexConvention.getEntityIndexName("chart")).thenReturn("datahub_chart_v2");

    // First two calls succeed, third call throws IOException
    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null) // First index succeeds
        .thenReturn(null) // Second index succeeds
        .thenThrow(
            new IOException("Failed to disable refresh for last index")); // Third index fails

    // Should throw IOException when last index fails
    assertThrows(IOException.class, () -> indexManager.disableRefresh());

    // Verify refresh is still disabled = false since operation failed
    assertFalse(indexManager.isRefreshDisabled());
  }

  @Test
  public void testDisableRefreshWithIOExceptionOnSingleIndex() throws IOException {
    // Mock responses for index discovery
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");

    // Mock IOException during single index refresh disable
    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Failed to disable refresh for single index"));

    // Should throw IOException when single index fails
    assertThrows(IOException.class, () -> indexManager.disableRefresh());

    // Verify refresh is still disabled = false since operation failed
    assertFalse(indexManager.isRefreshDisabled());
  }

  @Test
  public void testDisableRefreshWithMultipleIOExceptions() throws IOException {
    // Mock responses for index discovery
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2", "datahub_dashboard_v2", "datahub_chart_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    entitySpecs.put("dashboard", mock(com.linkedin.metadata.models.EntitySpec.class));
    entitySpecs.put("chart", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
    when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");
    when(mockIndexConvention.getEntityIndexName("chart")).thenReturn("datahub_chart_v2");

    // First call succeeds, second and third calls throw IOException
    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null) // First index succeeds
        .thenThrow(
            new IOException("Failed to disable refresh for second index")) // Second index fails
        .thenThrow(
            new IOException("Failed to disable refresh for third index")); // Third index fails

    // Should throw IOException when second index fails (first failure encountered)
    assertThrows(IOException.class, () -> indexManager.disableRefresh());

    // Verify refresh is still disabled = false since operation failed
    assertFalse(indexManager.isRefreshDisabled());
  }

  @Test
  public void testDisableRefreshWithEmptyIndicesList() throws IOException {
    // Mock responses for index discovery - no DataHub indices found
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"system_index", "other_index"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return empty entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    // Should not throw exception when no indices to process
    indexManager.disableRefresh();

    // Verify refresh is disabled = true since no indices to process
    assertTrue(indexManager.isRefreshDisabled());
  }

  @Test
  public void testIsDataHubIndexSkipsSystemIndices() {
    // Test with reflection to access private method
    try {
      var method = LoadIndicesIndexManager.class.getDeclaredMethod("isDataHubIndex", String.class);
      method.setAccessible(true);

      // Mock EntityRegistry to return entity specs
      Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
      entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
      when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

      // Mock index convention
      when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");

      // Test system indices (starting with ".")
      assertFalse((Boolean) method.invoke(indexManager, ".kibana"));
      assertFalse((Boolean) method.invoke(indexManager, ".security"));
      assertFalse((Boolean) method.invoke(indexManager, ".watches"));
      assertFalse((Boolean) method.invoke(indexManager, ".kibana_1"));
      assertFalse((Boolean) method.invoke(indexManager, ".system_index"));

      // Test that regular DataHub indices still work
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dataset_v2"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndex via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testIsDataHubIndexSkipsInternalElasticsearchIndices() {
    // Test with reflection to access private method
    try {
      var method = LoadIndicesIndexManager.class.getDeclaredMethod("isDataHubIndex", String.class);
      method.setAccessible(true);

      // Mock EntityRegistry to return entity specs
      Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
      entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
      when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

      // Mock index convention
      when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");

      // Test internal Elasticsearch indices (starting with "_")
      assertFalse((Boolean) method.invoke(indexManager, "_cluster"));
      assertFalse((Boolean) method.invoke(indexManager, "_nodes"));
      assertFalse((Boolean) method.invoke(indexManager, "_stats"));
      assertFalse((Boolean) method.invoke(indexManager, "_cat"));
      assertFalse((Boolean) method.invoke(indexManager, "_internal_index"));

      // Test that regular DataHub indices still work
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dataset_v2"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndex via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testIsDataHubIndexSkipsBothSystemAndInternalIndices() {
    // Test with reflection to access private method
    try {
      var method = LoadIndicesIndexManager.class.getDeclaredMethod("isDataHubIndex", String.class);
      method.setAccessible(true);

      // Mock EntityRegistry to return entity specs
      Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
      entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
      when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

      // Mock index convention
      when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");

      // Test various combinations of system and internal indices
      assertFalse((Boolean) method.invoke(indexManager, ".kibana"));
      assertFalse((Boolean) method.invoke(indexManager, "_cluster"));
      assertFalse((Boolean) method.invoke(indexManager, ".security"));
      assertFalse((Boolean) method.invoke(indexManager, "_nodes"));
      assertFalse((Boolean) method.invoke(indexManager, ".watches"));
      assertFalse((Boolean) method.invoke(indexManager, "_stats"));
      assertFalse((Boolean) method.invoke(indexManager, ".system_index"));
      assertFalse((Boolean) method.invoke(indexManager, "_internal_index"));

      // Test edge cases
      assertFalse((Boolean) method.invoke(indexManager, "."));
      assertFalse((Boolean) method.invoke(indexManager, "_"));
      assertFalse((Boolean) method.invoke(indexManager, ".a"));
      assertFalse((Boolean) method.invoke(indexManager, "_a"));

      // Test that regular DataHub indices still work
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dataset_v2"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndex via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testDiscoverDataHubIndicesFiltersSystemAndInternalIndices() throws IOException {
    // Mock responses for index discovery
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {
      "datahub_dataset_v2", // Should be included
      ".kibana", // Should be filtered out (system)
      "_cluster", // Should be filtered out (internal)
      "datahub_dashboard_v2", // Should be included
      ".security", // Should be filtered out (system)
      "_nodes", // Should be filtered out (internal)
      "other_index" // Should be filtered out (not DataHub)
    };
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
    entitySpecs.put("dashboard", mock(com.linkedin.metadata.models.EntitySpec.class));
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    // Mock index convention
    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
    when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");

    Set<String> result = indexManager.discoverDataHubIndices();

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains("datahub_dataset_v2"));
    assertTrue(result.contains("datahub_dashboard_v2"));
    assertFalse(result.contains(".kibana"));
    assertFalse(result.contains("_cluster"));
    assertFalse(result.contains(".security"));
    assertFalse(result.contains("_nodes"));
    assertFalse(result.contains("other_index"));
  }

  @Test
  public void testDiscoverDataHubIndicesWithOnlySystemAndInternalIndices() throws IOException {
    // Mock responses for index discovery - only system and internal indices
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {".kibana", ".security", ".watches", "_cluster", "_nodes", "_stats"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return empty entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    Set<String> result = indexManager.discoverDataHubIndices();

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testIsDataHubIndexWithMixedIndexNames() {
    // Test with reflection to access private method
    try {
      var method = LoadIndicesIndexManager.class.getDeclaredMethod("isDataHubIndex", String.class);
      method.setAccessible(true);

      // Mock EntityRegistry to return entity specs
      Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
      entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
      entitySpecs.put("dashboard", mock(com.linkedin.metadata.models.EntitySpec.class));
      when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

      // Mock index convention
      when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
      when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");

      // Test various index name patterns
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dataset_v2"));
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dashboard_v2"));

      // System indices should be filtered
      assertFalse((Boolean) method.invoke(indexManager, ".kibana"));
      assertFalse((Boolean) method.invoke(indexManager, ".security"));

      // Internal indices should be filtered
      assertFalse((Boolean) method.invoke(indexManager, "_cluster"));
      assertFalse((Boolean) method.invoke(indexManager, "_nodes"));

      // Non-DataHub indices should be filtered
      assertFalse((Boolean) method.invoke(indexManager, "random_index"));
      assertFalse((Boolean) method.invoke(indexManager, "other_system"));

      // Edge cases
      assertFalse((Boolean) method.invoke(indexManager, ""));
      assertFalse((Boolean) method.invoke(indexManager, "datahub_dataset_v2_with_dot."));
      assertFalse((Boolean) method.invoke(indexManager, "datahub_dataset_v2_with_underscore_"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndex via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testIsDataHubIndexByConventionWithTimeseriesAspectIndices() {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod(
              "isDataHubIndexByConvention", String.class);
      method.setAccessible(true);

      // Create mock entity specs with aspect specs
      com.linkedin.metadata.models.EntitySpec mockEntitySpec1 =
          mock(com.linkedin.metadata.models.EntitySpec.class);
      com.linkedin.metadata.models.EntitySpec mockEntitySpec2 =
          mock(com.linkedin.metadata.models.EntitySpec.class);

      com.linkedin.metadata.models.AspectSpec mockAspectSpec1 =
          mock(com.linkedin.metadata.models.AspectSpec.class);
      com.linkedin.metadata.models.AspectSpec mockAspectSpec2 =
          mock(com.linkedin.metadata.models.AspectSpec.class);
      com.linkedin.metadata.models.AspectSpec mockAspectSpec3 =
          mock(com.linkedin.metadata.models.AspectSpec.class);

      // Setup entity registry mock
      Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
      entitySpecs.put("dataset", mockEntitySpec1);
      entitySpecs.put("dashboard", mockEntitySpec2);
      when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

      when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec1);
      when(mockEntityRegistry.getEntitySpec("dashboard")).thenReturn(mockEntitySpec2);

      // Setup entity specs with aspect specs
      when(mockEntitySpec1.getAspectSpecs())
          .thenReturn(java.util.List.of(mockAspectSpec1, mockAspectSpec2));
      when(mockEntitySpec2.getAspectSpecs()).thenReturn(java.util.List.of(mockAspectSpec3));

      // Setup aspect specs
      when(mockAspectSpec1.getName()).thenReturn("datasetProfile");
      when(mockAspectSpec2.getName()).thenReturn("datasetUsageStatistics");
      when(mockAspectSpec3.getName()).thenReturn("dashboardUsageStatistics");

      // Mock index convention for timeseries aspect indices
      when(mockIndexConvention.getTimeseriesAspectIndexName("dataset", "datasetProfile"))
          .thenReturn("datahub_dataset_datasetprofile_timeseries");
      when(mockIndexConvention.getTimeseriesAspectIndexName("dataset", "datasetUsageStatistics"))
          .thenReturn("datahub_dataset_datasetusagestatistics_timeseries");
      when(mockIndexConvention.getTimeseriesAspectIndexName(
              "dashboard", "dashboardUsageStatistics"))
          .thenReturn("datahub_dashboard_dashboardusagestatistics_timeseries");

      // Test timeseries aspect indices
      assertTrue(
          (Boolean) method.invoke(indexManager, "datahub_dataset_datasetprofile_timeseries"));
      assertTrue(
          (Boolean)
              method.invoke(indexManager, "datahub_dataset_datasetusagestatistics_timeseries"));
      assertTrue(
          (Boolean)
              method.invoke(indexManager, "datahub_dashboard_dashboardusagestatistics_timeseries"));

      // Test non-matching indices
      assertFalse((Boolean) method.invoke(indexManager, "datahub_dataset_nonexistent_timeseries"));
      assertFalse((Boolean) method.invoke(indexManager, "random_timeseries_index"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndexByConvention via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testIsDataHubIndexByConventionWithMixedEntityAndTimeseriesIndices() {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod(
              "isDataHubIndexByConvention", String.class);
      method.setAccessible(true);

      // Create mock entity specs with aspect specs
      com.linkedin.metadata.models.EntitySpec mockEntitySpec1 =
          mock(com.linkedin.metadata.models.EntitySpec.class);
      com.linkedin.metadata.models.EntitySpec mockEntitySpec2 =
          mock(com.linkedin.metadata.models.EntitySpec.class);

      com.linkedin.metadata.models.AspectSpec mockAspectSpec1 =
          mock(com.linkedin.metadata.models.AspectSpec.class);
      com.linkedin.metadata.models.AspectSpec mockAspectSpec2 =
          mock(com.linkedin.metadata.models.AspectSpec.class);

      // Setup entity registry mock
      Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
      entitySpecs.put("dataset", mockEntitySpec1);
      entitySpecs.put("dashboard", mockEntitySpec2);
      when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

      when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec1);
      when(mockEntityRegistry.getEntitySpec("dashboard")).thenReturn(mockEntitySpec2);

      // Setup entity specs with aspect specs
      when(mockEntitySpec1.getAspectSpecs()).thenReturn(java.util.List.of(mockAspectSpec1));
      when(mockEntitySpec2.getAspectSpecs()).thenReturn(java.util.List.of(mockAspectSpec2));

      // Setup aspect specs
      when(mockAspectSpec1.getName()).thenReturn("datasetProfile");
      when(mockAspectSpec2.getName()).thenReturn("dashboardUsageStatistics");

      // Mock index convention for both entity and timeseries aspect indices
      when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
      when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");

      when(mockIndexConvention.getTimeseriesAspectIndexName("dataset", "datasetProfile"))
          .thenReturn("datahub_dataset_datasetprofile_timeseries");
      when(mockIndexConvention.getTimeseriesAspectIndexName(
              "dashboard", "dashboardUsageStatistics"))
          .thenReturn("datahub_dashboard_dashboardusagestatistics_timeseries");

      // Test entity indices
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dataset_v2"));
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dashboard_v2"));

      // Test timeseries aspect indices
      assertTrue(
          (Boolean) method.invoke(indexManager, "datahub_dataset_datasetprofile_timeseries"));
      assertTrue(
          (Boolean)
              method.invoke(indexManager, "datahub_dashboard_dashboardusagestatistics_timeseries"));

      // Test non-matching indices
      assertFalse((Boolean) method.invoke(indexManager, "datahub_dataset_nonexistent_timeseries"));
      assertFalse((Boolean) method.invoke(indexManager, "random_index"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndexByConvention via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testIsDataHubIndexByConventionWithEmptyAspectSpecs() {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod(
              "isDataHubIndexByConvention", String.class);
      method.setAccessible(true);

      // Create mock entity spec with no aspect specs
      com.linkedin.metadata.models.EntitySpec mockEntitySpec =
          mock(com.linkedin.metadata.models.EntitySpec.class);

      // Setup entity registry mock
      Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
      entitySpecs.put("dataset", mockEntitySpec);
      when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

      when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);

      // Setup entity spec with empty aspect specs
      when(mockEntitySpec.getAspectSpecs()).thenReturn(java.util.List.of());

      // Mock index convention for entity index only
      when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");

      // Test entity index
      assertTrue((Boolean) method.invoke(indexManager, "datahub_dataset_v2"));

      // Test timeseries aspect index - should not match since no aspect specs
      assertFalse((Boolean) method.invoke(indexManager, "datahub_dataset_profile_timeseries"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndexByConvention via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testIsDataHubIndexByConventionWithExceptionHandling() {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod(
              "isDataHubIndexByConvention", String.class);
      method.setAccessible(true);

      // Mock entity registry to throw exception
      when(mockEntityRegistry.getEntitySpecs()).thenThrow(new RuntimeException("Registry error"));

      // Should return false when exception occurs
      assertFalse((Boolean) method.invoke(indexManager, "any_index"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndexByConvention via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testIsDataHubIndexByConventionWithEntitySpecException() {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod(
              "isDataHubIndexByConvention", String.class);
      method.setAccessible(true);

      // Setup entity registry mock
      Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
      entitySpecs.put("dataset", mock(com.linkedin.metadata.models.EntitySpec.class));
      when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

      // Mock getEntitySpec to throw exception
      when(mockEntityRegistry.getEntitySpec("dataset"))
          .thenThrow(new RuntimeException("Entity spec error"));

      // Should return false when exception occurs
      assertFalse((Boolean) method.invoke(indexManager, "any_index"));

    } catch (Exception e) {
      fail("Failed to test isDataHubIndexByConvention via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testDiscoverDataHubIndicesWithTimeseriesAspectIndices() throws IOException {
    // Mock responses for index discovery
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {
      "datahub_dataset_v2", // Entity index
      "datahub_dataset_datasetprofile_timeseries", // Timeseries aspect index
      "datahub_dataset_datasetusagestatistics_timeseries", // Timeseries aspect index
      "datahub_dashboard_v2", // Entity index
      "datahub_dashboard_dashboardusagestatistics_timeseries", // Timeseries aspect index
      ".kibana", // System index (filtered)
      "_cluster", // Internal index (filtered)
      "other_index" // Non-DataHub index (filtered)
    };
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Create mock entity specs with aspect specs
    com.linkedin.metadata.models.EntitySpec mockEntitySpec1 =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    com.linkedin.metadata.models.EntitySpec mockEntitySpec2 =
        mock(com.linkedin.metadata.models.EntitySpec.class);

    com.linkedin.metadata.models.AspectSpec mockAspectSpec1 =
        mock(com.linkedin.metadata.models.AspectSpec.class);
    com.linkedin.metadata.models.AspectSpec mockAspectSpec2 =
        mock(com.linkedin.metadata.models.AspectSpec.class);
    com.linkedin.metadata.models.AspectSpec mockAspectSpec3 =
        mock(com.linkedin.metadata.models.AspectSpec.class);

    // Setup entity registry mock
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mockEntitySpec1);
    entitySpecs.put("dashboard", mockEntitySpec2);
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec1);
    when(mockEntityRegistry.getEntitySpec("dashboard")).thenReturn(mockEntitySpec2);

    // Setup entity specs with aspect specs
    when(mockEntitySpec1.getAspectSpecs())
        .thenReturn(java.util.List.of(mockAspectSpec1, mockAspectSpec2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(java.util.List.of(mockAspectSpec3));

    // Setup aspect specs
    when(mockAspectSpec1.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec2.getName()).thenReturn("datasetUsageStatistics");
    when(mockAspectSpec3.getName()).thenReturn("dashboardUsageStatistics");

    // Mock index convention for both entity and timeseries aspect indices
    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
    when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");

    when(mockIndexConvention.getTimeseriesAspectIndexName("dataset", "datasetProfile"))
        .thenReturn("datahub_dataset_datasetprofile_timeseries");
    when(mockIndexConvention.getTimeseriesAspectIndexName("dataset", "datasetUsageStatistics"))
        .thenReturn("datahub_dataset_datasetusagestatistics_timeseries");
    when(mockIndexConvention.getTimeseriesAspectIndexName("dashboard", "dashboardUsageStatistics"))
        .thenReturn("datahub_dashboard_dashboardusagestatistics_timeseries");

    Set<String> result = indexManager.discoverDataHubIndices();

    assertNotNull(result);
    assertEquals(result.size(), 5);
    assertTrue(result.contains("datahub_dataset_v2"));
    assertTrue(result.contains("datahub_dataset_datasetprofile_timeseries"));
    assertTrue(result.contains("datahub_dataset_datasetusagestatistics_timeseries"));
    assertTrue(result.contains("datahub_dashboard_v2"));
    assertTrue(result.contains("datahub_dashboard_dashboardusagestatistics_timeseries"));

    // Verify filtered indices are not included
    assertFalse(result.contains(".kibana"));
    assertFalse(result.contains("_cluster"));
    assertFalse(result.contains("other_index"));
  }

  @Test
  public void testGetIndexRefreshIntervalSuccess() throws IOException {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod("getIndexRefreshInterval", String.class);
      method.setAccessible(true);

      String indexName = "datahub_dataset_v2";
      String expectedRefreshInterval = "1s";

      // Mock GetSettingsResponse
      GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
      when(mockResponse.getSetting(indexName, "index.refresh_interval"))
          .thenReturn(expectedRefreshInterval);

      // Mock search client
      when(mockSearchClient.getIndexSettings(
              any(GetSettingsRequest.class), any(RequestOptions.class)))
          .thenReturn(mockResponse);

      String result = (String) method.invoke(indexManager, indexName);

      assertEquals(result, expectedRefreshInterval);
      verify(mockSearchClient)
          .getIndexSettings(any(GetSettingsRequest.class), any(RequestOptions.class));

    } catch (Exception e) {
      fail("Failed to test getIndexRefreshInterval via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testGetIndexRefreshIntervalWithDifferentValues() throws IOException {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod("getIndexRefreshInterval", String.class);
      method.setAccessible(true);

      // Test different refresh interval values
      String[] indexNames = {"datahub_dataset_v2", "datahub_dashboard_v2", "datahub_chart_v2"};
      String[] refreshIntervals = {"1s", "5s", "-1"}; // enabled, enabled, disabled

      for (int i = 0; i < indexNames.length; i++) {
        String indexName = indexNames[i];
        String expectedRefreshInterval = refreshIntervals[i];

        // Mock GetSettingsResponse
        GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
        when(mockResponse.getSetting(indexName, "index.refresh_interval"))
            .thenReturn(expectedRefreshInterval);

        // Mock search client
        when(mockSearchClient.getIndexSettings(
                any(GetSettingsRequest.class), any(RequestOptions.class)))
            .thenReturn(mockResponse);

        String result = (String) method.invoke(indexManager, indexName);

        assertEquals(result, expectedRefreshInterval, "Failed for index: " + indexName);
      }

    } catch (Exception e) {
      fail("Failed to test getIndexRefreshInterval via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testGetIndexRefreshIntervalWithIOException() throws IOException {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod("getIndexRefreshInterval", String.class);
      method.setAccessible(true);

      String indexName = "datahub_dataset_v2";

      // Mock search client to throw IOException
      when(mockSearchClient.getIndexSettings(
              any(GetSettingsRequest.class), any(RequestOptions.class)))
          .thenThrow(new IOException("Failed to get index settings"));

      // Should throw IOException
      assertThrows(Exception.class, () -> method.invoke(indexManager, indexName));

    } catch (Exception e) {
      fail("Failed to test getIndexRefreshInterval via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testGetIndexRefreshIntervalWithNullResponse() throws IOException {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod("getIndexRefreshInterval", String.class);
      method.setAccessible(true);

      String indexName = "datahub_dataset_v2";

      // Mock GetSettingsResponse that returns null
      GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
      when(mockResponse.getSetting(indexName, "index.refresh_interval")).thenReturn(null);

      // Mock search client
      when(mockSearchClient.getIndexSettings(
              any(GetSettingsRequest.class), any(RequestOptions.class)))
          .thenReturn(mockResponse);

      String result = (String) method.invoke(indexManager, indexName);

      assertNull(result);

    } catch (Exception e) {
      fail("Failed to test getIndexRefreshInterval via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testGetIndexRefreshIntervalWithEmptyString() throws IOException {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod("getIndexRefreshInterval", String.class);
      method.setAccessible(true);

      String indexName = "datahub_dataset_v2";
      String expectedRefreshInterval = "";

      // Mock GetSettingsResponse
      GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
      when(mockResponse.getSetting(indexName, "index.refresh_interval"))
          .thenReturn(expectedRefreshInterval);

      // Mock search client
      when(mockSearchClient.getIndexSettings(
              any(GetSettingsRequest.class), any(RequestOptions.class)))
          .thenReturn(mockResponse);

      String result = (String) method.invoke(indexManager, indexName);

      assertEquals(result, expectedRefreshInterval);

    } catch (Exception e) {
      fail("Failed to test getIndexRefreshInterval via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testGetIndexRefreshIntervalRequestConfiguration() throws IOException {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod("getIndexRefreshInterval", String.class);
      method.setAccessible(true);

      String indexName = "datahub_dataset_v2";
      String expectedRefreshInterval = "3s";

      // Mock GetSettingsResponse
      GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
      when(mockResponse.getSetting(indexName, "index.refresh_interval"))
          .thenReturn(expectedRefreshInterval);

      // Mock search client with argument capture
      when(mockSearchClient.getIndexSettings(
              any(GetSettingsRequest.class), any(RequestOptions.class)))
          .thenReturn(mockResponse);

      method.invoke(indexManager, indexName);

      // Verify the request was configured correctly
      ArgumentCaptor<GetSettingsRequest> requestCaptor =
          ArgumentCaptor.forClass(GetSettingsRequest.class);
      verify(mockSearchClient).getIndexSettings(requestCaptor.capture(), any(RequestOptions.class));

      GetSettingsRequest capturedRequest = requestCaptor.getValue();
      assertNotNull(capturedRequest);
      // Note: We can't easily verify the internal configuration of GetSettingsRequest
      // as it doesn't expose getters for the configuration, but we can verify it was called

    } catch (Exception e) {
      fail("Failed to test getIndexRefreshInterval via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testGetIndexRefreshIntervalWithSpecialCharacters() throws IOException {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod("getIndexRefreshInterval", String.class);
      method.setAccessible(true);

      // Test with index names containing special characters
      String[] indexNames = {
        "datahub_dataset_v2",
        "datahub-dashboard-v2",
        "datahub_dataset_profile_timeseries",
        "datahub_dataset.usagestatistics.timeseries"
      };
      String expectedRefreshInterval = "2s";

      for (String indexName : indexNames) {
        // Mock GetSettingsResponse
        GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
        when(mockResponse.getSetting(indexName, "index.refresh_interval"))
            .thenReturn(expectedRefreshInterval);

        // Mock search client
        when(mockSearchClient.getIndexSettings(
                any(GetSettingsRequest.class), any(RequestOptions.class)))
            .thenReturn(mockResponse);

        String result = (String) method.invoke(indexManager, indexName);

        assertEquals(result, expectedRefreshInterval, "Failed for index: " + indexName);
      }

    } catch (Exception e) {
      fail("Failed to test getIndexRefreshInterval via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testGetIndexRefreshIntervalWithDefaultValue() throws IOException {
    // Test with reflection to access private method
    try {
      var method =
          LoadIndicesIndexManager.class.getDeclaredMethod("getIndexRefreshInterval", String.class);
      method.setAccessible(true);

      String indexName = "datahub_dataset_v2";
      String expectedRefreshInterval = "1s"; // Default Elasticsearch refresh interval

      // Mock GetSettingsResponse
      GetSettingsResponse mockResponse = mock(GetSettingsResponse.class);
      when(mockResponse.getSetting(indexName, "index.refresh_interval"))
          .thenReturn(expectedRefreshInterval);

      // Mock search client
      when(mockSearchClient.getIndexSettings(
              any(GetSettingsRequest.class), any(RequestOptions.class)))
          .thenReturn(mockResponse);

      String result = (String) method.invoke(indexManager, indexName);

      assertEquals(result, expectedRefreshInterval);

    } catch (Exception e) {
      fail("Failed to test getIndexRefreshInterval via reflection: " + e.getMessage());
    }
  }

  @Test
  public void testGetManagedIndexCount() throws IOException {
    // Mock responses for index discovery
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {
      "datahub_dataset_v2", // Entity index
      "datahub_dataset_datasetprofile_timeseries", // Timeseries aspect index
      "datahub_dashboard_v2", // Entity index
      ".kibana", // System index (filtered)
      "_cluster", // Internal index (filtered)
      "other_index" // Non-DataHub index (filtered)
    };
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Create mock entity specs with aspect specs
    com.linkedin.metadata.models.EntitySpec mockEntitySpec1 =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    com.linkedin.metadata.models.EntitySpec mockEntitySpec2 =
        mock(com.linkedin.metadata.models.EntitySpec.class);

    com.linkedin.metadata.models.AspectSpec mockAspectSpec1 =
        mock(com.linkedin.metadata.models.AspectSpec.class);

    // Setup entity registry mock
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mockEntitySpec1);
    entitySpecs.put("dashboard", mockEntitySpec2);
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec1);
    when(mockEntityRegistry.getEntitySpec("dashboard")).thenReturn(mockEntitySpec2);

    // Setup entity specs with aspect specs
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(java.util.List.of(mockAspectSpec1));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(java.util.List.of());

    // Setup aspect specs
    when(mockAspectSpec1.getName()).thenReturn("datasetProfile");

    // Mock index convention for both entity and timeseries aspect indices
    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
    when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");

    when(mockIndexConvention.getTimeseriesAspectIndexName("dataset", "datasetProfile"))
        .thenReturn("datahub_dataset_datasetprofile_timeseries");

    int result = indexManager.getManagedIndexCount();

    // Should return 3: 2 entity indices + 1 timeseries aspect index
    assertEquals(result, 3);
  }

  @Test
  public void testGetManagedIndexCountWithEmptyIndices() throws IOException {
    // Mock responses for index discovery - no DataHub indices
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {".kibana", ".security", "_cluster", "_nodes", "other_index"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock EntityRegistry to return empty entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    int result = indexManager.getManagedIndexCount();

    assertEquals(result, 0);
  }

  @Test
  public void testGetManagedIndexCountWithOnlyEntityIndices() throws IOException {
    // Mock responses for index discovery - only entity indices
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2", "datahub_dashboard_v2", "datahub_chart_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Create mock entity specs
    com.linkedin.metadata.models.EntitySpec mockEntitySpec1 =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    com.linkedin.metadata.models.EntitySpec mockEntitySpec2 =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    com.linkedin.metadata.models.EntitySpec mockEntitySpec3 =
        mock(com.linkedin.metadata.models.EntitySpec.class);

    // Setup entity registry mock
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mockEntitySpec1);
    entitySpecs.put("dashboard", mockEntitySpec2);
    entitySpecs.put("chart", mockEntitySpec3);
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec1);
    when(mockEntityRegistry.getEntitySpec("dashboard")).thenReturn(mockEntitySpec2);
    when(mockEntityRegistry.getEntitySpec("chart")).thenReturn(mockEntitySpec3);

    // Setup entity specs with no aspect specs (no timeseries indices)
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(java.util.List.of());
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(java.util.List.of());
    when(mockEntitySpec3.getAspectSpecs()).thenReturn(java.util.List.of());

    // Mock index convention for entity indices only
    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datahub_dataset_v2");
    when(mockIndexConvention.getEntityIndexName("dashboard")).thenReturn("datahub_dashboard_v2");
    when(mockIndexConvention.getEntityIndexName("chart")).thenReturn("datahub_chart_v2");

    int result = indexManager.getManagedIndexCount();

    assertEquals(result, 3);
  }

  @Test
  public void testGetManagedIndexCountWithOnlyTimeseriesIndices() throws IOException {
    // Mock responses for index discovery - only timeseries aspect indices
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {
      "datahub_dataset_datasetprofile_timeseries",
      "datahub_dataset_datasetusagestatistics_timeseries",
      "datahub_dashboard_dashboardusagestatistics_timeseries"
    };
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Create mock entity specs with aspect specs
    com.linkedin.metadata.models.EntitySpec mockEntitySpec1 =
        mock(com.linkedin.metadata.models.EntitySpec.class);
    com.linkedin.metadata.models.EntitySpec mockEntitySpec2 =
        mock(com.linkedin.metadata.models.EntitySpec.class);

    com.linkedin.metadata.models.AspectSpec mockAspectSpec1 =
        mock(com.linkedin.metadata.models.AspectSpec.class);
    com.linkedin.metadata.models.AspectSpec mockAspectSpec2 =
        mock(com.linkedin.metadata.models.AspectSpec.class);
    com.linkedin.metadata.models.AspectSpec mockAspectSpec3 =
        mock(com.linkedin.metadata.models.AspectSpec.class);

    // Setup entity registry mock
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", mockEntitySpec1);
    entitySpecs.put("dashboard", mockEntitySpec2);
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec1);
    when(mockEntityRegistry.getEntitySpec("dashboard")).thenReturn(mockEntitySpec2);

    // Setup entity specs with aspect specs
    when(mockEntitySpec1.getAspectSpecs())
        .thenReturn(java.util.List.of(mockAspectSpec1, mockAspectSpec2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(java.util.List.of(mockAspectSpec3));

    // Setup aspect specs
    when(mockAspectSpec1.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec2.getName()).thenReturn("datasetUsageStatistics");
    when(mockAspectSpec3.getName()).thenReturn("dashboardUsageStatistics");

    // Mock index convention for timeseries aspect indices only
    when(mockIndexConvention.getTimeseriesAspectIndexName("dataset", "datasetProfile"))
        .thenReturn("datahub_dataset_datasetprofile_timeseries");
    when(mockIndexConvention.getTimeseriesAspectIndexName("dataset", "datasetUsageStatistics"))
        .thenReturn("datahub_dataset_datasetusagestatistics_timeseries");
    when(mockIndexConvention.getTimeseriesAspectIndexName("dashboard", "dashboardUsageStatistics"))
        .thenReturn("datahub_dashboard_dashboardusagestatistics_timeseries");

    int result = indexManager.getManagedIndexCount();

    assertEquals(result, 3);
  }

  @Test
  public void testGetManagedIndexCountWithIOException() throws IOException {
    // Mock search client to throw IOException
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Failed to get indices"));

    // Should throw IOException
    assertThrows(IOException.class, () -> indexManager.getManagedIndexCount());
  }

  @Test
  public void testGetManagedIndexCountWithLargeNumberOfIndices() throws IOException {
    // Mock responses for index discovery - large number of indices
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = new String[100];
    for (int i = 0; i < 100; i++) {
      allIndices[i] = "datahub_dataset_" + i + "_v2";
    }
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Create mock entity specs
    Map<String, com.linkedin.metadata.models.EntitySpec> entitySpecs = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      com.linkedin.metadata.models.EntitySpec mockEntitySpec =
          mock(com.linkedin.metadata.models.EntitySpec.class);
      entitySpecs.put("dataset" + i, mockEntitySpec);
      when(mockEntityRegistry.getEntitySpec("dataset" + i)).thenReturn(mockEntitySpec);
      when(mockEntitySpec.getAspectSpecs()).thenReturn(java.util.List.of());
      when(mockIndexConvention.getEntityIndexName("dataset" + i))
          .thenReturn("datahub_dataset_" + i + "_v2");
    }
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecs);

    int result = indexManager.getManagedIndexCount();

    assertEquals(result, 100);
  }
}
