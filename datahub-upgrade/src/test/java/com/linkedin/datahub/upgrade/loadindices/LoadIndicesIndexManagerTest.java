package com.linkedin.datahub.upgrade.loadindices;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadIndicesIndexManagerTest {

  private LoadIndicesIndexManager indexManager;
  private RestHighLevelClient mockSearchClient;
  private IndicesClient mockIndicesClient;
  private IndexConvention mockIndexConvention;
  private EntityRegistry mockEntityRegistry;
  private String configuredRefreshInterval;

  @BeforeMethod
  public void setUp() {
    mockSearchClient = mock(RestHighLevelClient.class);
    mockIndicesClient = mock(IndicesClient.class);
    mockIndexConvention = mock(IndexConvention.class);
    mockEntityRegistry = mock(EntityRegistry.class);
    configuredRefreshInterval = "3s";

    // Mock the indices() method to return our mock IndicesClient
    when(mockSearchClient.indices()).thenReturn(mockIndicesClient);

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
    when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
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
    when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Connection failed"));

    assertThrows(IOException.class, () -> indexManager.discoverDataHubIndices());
  }

  @Test
  public void testDisableRefresh() throws IOException {
    // Mock discoverDataHubIndices behavior
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
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

    when(mockIndicesClient.getSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSettingsResponse);

    // Mock update settings
    when(mockIndicesClient.putSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null);

    indexManager.disableRefresh();

    assertTrue(indexManager.isRefreshDisabled());
  }

  @Test
  public void testDisableRefreshWithIOException() throws IOException {
    // Mock IOException during discovery
    when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Discovery failed"));

    assertThrows(IOException.class, () -> indexManager.disableRefresh());
  }

  @Test
  public void testRestoreRefresh() throws IOException {
    // First disable refresh to set up state
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
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

    when(mockIndicesClient.getSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSettingsResponse);

    when(mockIndicesClient.putSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class)))
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
    when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
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

    when(mockIndicesClient.getSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSettingsResponse);

    when(mockIndicesClient.putSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null);

    // Disable refresh first
    indexManager.disableRefresh();
    assertTrue(indexManager.isRefreshDisabled());

    // Mock IOException during restore
    when(mockIndicesClient.putSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class)))
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
    when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
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

    when(mockIndicesClient.getSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSettingsResponse);

    when(mockIndicesClient.putSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null);

    // First call
    indexManager.disableRefresh();
    assertTrue(indexManager.isRefreshDisabled());

    // Second call should not discover indices again
    indexManager.disableRefresh();
    assertTrue(indexManager.isRefreshDisabled());

    // Verify discovery was called only once
    verify(mockIndicesClient, times(1)).get(any(GetIndexRequest.class), any(RequestOptions.class));
  }

  @Test
  public void testEmptyIndicesDiscovery() throws IOException {
    // Mock empty response
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    when(mockResponse.getIndices()).thenReturn(new String[0]);
    when(mockIndicesClient.get(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    Set<String> result = indexManager.discoverDataHubIndices();

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }
}
