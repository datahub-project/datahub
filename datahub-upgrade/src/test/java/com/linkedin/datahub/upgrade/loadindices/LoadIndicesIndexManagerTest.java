package com.linkedin.datahub.upgrade.loadindices;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadIndicesIndexManagerTest {

  private LoadIndicesIndexManager indexManager;
  private SearchClientShim<?> mockSearchClient;
  private IndexConvention mockIndexConvention;
  private ESIndexBuilder mockIndexBuilder;

  @BeforeMethod
  public void setUp() {
    mockSearchClient = mock(SearchClientShim.class);
    mockIndexConvention = mock(IndexConvention.class);
    mockIndexBuilder = mock(ESIndexBuilder.class);

    // Create a fresh instance for each test to avoid state accumulation
    indexManager =
        new LoadIndicesIndexManager(mockSearchClient, mockIndexConvention, mockIndexBuilder);
  }

  @Test
  public void testConstructor() {
    assertNotNull(indexManager);
    assertFalse(indexManager.isSettingsOptimized());
  }

  @Test
  public void testDiscoverDataHubIndexConfigs() throws IOException {
    // Mock entity indices response
    GetIndexResponse mockEntityResponse = mock(GetIndexResponse.class);
    String[] entityIndices = {
      "datahub_datasetindex_v2", "datahub_dashboardindex_v2", "datahub_chartindex_v2"
    };
    when(mockEntityResponse.getIndices()).thenReturn(entityIndices);

    // Mock graph service index response
    GetIndexResponse mockGraphResponse = mock(GetIndexResponse.class);
    String[] graphIndices = {"datahub_graph_service_v1"};
    when(mockGraphResponse.getIndices()).thenReturn(graphIndices);

    // Mock system metadata index response
    GetIndexResponse mockSystemMetadataResponse = mock(GetIndexResponse.class);
    String[] systemMetadataIndices = {"datahub_system_metadata_service_v1"};
    when(mockSystemMetadataResponse.getIndices()).thenReturn(systemMetadataIndices);

    // Mock the search client to return different responses for different indices
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockEntityResponse)
        .thenReturn(mockGraphResponse)
        .thenReturn(mockSystemMetadataResponse);

    // Mock index convention patterns
    when(mockIndexConvention.getAllEntityIndicesPatterns())
        .thenReturn(List.of("datahub_*index_v2"));
    when(mockIndexConvention.getIndexName(ElasticSearchGraphService.INDEX_NAME))
        .thenReturn("datahub_graph_service_v1");
    when(mockIndexConvention.getIndexName(ElasticSearchSystemMetadataService.INDEX_NAME))
        .thenReturn("datahub_system_metadata_service_v1");

    // Mock ESIndexBuilder to return ReindexConfig objects
    ReindexConfig mockConfig1 = mock(ReindexConfig.class);
    ReindexConfig mockConfig2 = mock(ReindexConfig.class);
    ReindexConfig mockConfig3 = mock(ReindexConfig.class);
    ReindexConfig mockConfig4 = mock(ReindexConfig.class);
    ReindexConfig mockConfig5 = mock(ReindexConfig.class);

    when(mockConfig1.name()).thenReturn("datahub_datasetindex_v2");
    when(mockConfig2.name()).thenReturn("datahub_dashboardindex_v2");
    when(mockConfig3.name()).thenReturn("datahub_chartindex_v2");
    when(mockConfig4.name()).thenReturn("datahub_graph_service_v1");
    when(mockConfig5.name()).thenReturn("datahub_system_metadata_service_v1");

    // Mock target settings for each config
    Map<String, Object> targetSettings1 =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "3s", ESIndexBuilder.NUMBER_OF_REPLICAS, 1));
    Map<String, Object> targetSettings2 =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "3s", ESIndexBuilder.NUMBER_OF_REPLICAS, 1));
    Map<String, Object> targetSettings3 =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "3s", ESIndexBuilder.NUMBER_OF_REPLICAS, 1));
    Map<String, Object> targetSettings4 =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "3s", ESIndexBuilder.NUMBER_OF_REPLICAS, 1));
    Map<String, Object> targetSettings5 =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "3s", ESIndexBuilder.NUMBER_OF_REPLICAS, 1));

    when(mockConfig1.targetSettings()).thenReturn(targetSettings1);
    when(mockConfig2.targetSettings()).thenReturn(targetSettings2);
    when(mockConfig3.targetSettings()).thenReturn(targetSettings3);
    when(mockConfig4.targetSettings()).thenReturn(targetSettings4);
    when(mockConfig5.targetSettings()).thenReturn(targetSettings5);

    when(mockIndexBuilder.buildReindexState(any(String.class), any(Map.class), any(Map.class)))
        .thenReturn(mockConfig1)
        .thenReturn(mockConfig2)
        .thenReturn(mockConfig3)
        .thenReturn(mockConfig4)
        .thenReturn(mockConfig5);

    var result = indexManager.discoverDataHubIndexConfigs();

    assertNotNull(result);
    assertEquals(result.size(), 5);
    assertTrue(result.stream().anyMatch(c -> c.name().equals("datahub_datasetindex_v2")));
    assertTrue(result.stream().anyMatch(c -> c.name().equals("datahub_dashboardindex_v2")));
    assertTrue(result.stream().anyMatch(c -> c.name().equals("datahub_chartindex_v2")));
    assertTrue(result.stream().anyMatch(c -> c.name().equals("datahub_graph_service_v1")));
    assertTrue(
        result.stream().anyMatch(c -> c.name().equals("datahub_system_metadata_service_v1")));
  }

  @Test
  public void testDiscoverDataHubIndexConfigsWithIOException() throws IOException {
    // Mock getAllEntityIndicesPatterns to return a pattern so the loop executes
    when(mockIndexConvention.getAllEntityIndicesPatterns())
        .thenReturn(List.of("datahub_*index_v2"));

    // Mock IOException
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Connection failed"));

    assertThrows(IOException.class, () -> indexManager.discoverDataHubIndexConfigs());
  }

  @Test
  public void testOptimizeForBulkOperations() throws IOException {
    // Mock discoverDataHubIndexConfigs behavior
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock ESIndexBuilder to return ReindexConfig objects
    ReindexConfig mockConfig = mock(ReindexConfig.class);
    when(mockConfig.name()).thenReturn("datahub_dataset_v2");
    Map<String, Object> targetSettings =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "3s", ESIndexBuilder.NUMBER_OF_REPLICAS, 1));
    when(mockConfig.targetSettings()).thenReturn(targetSettings);
    when(mockIndexBuilder.buildReindexState(any(String.class), any(Map.class), any(Map.class)))
        .thenReturn(mockConfig);

    // Mock update settings
    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null);

    indexManager.optimizeForBulkOperations();

    assertTrue(indexManager.isSettingsOptimized());
  }

  @Test
  public void testOptimizeForBulkOperationsWithIOException() throws IOException {
    // Mock getAllEntityIndicesPatterns to return a pattern so the loop executes
    when(mockIndexConvention.getAllEntityIndicesPatterns())
        .thenReturn(List.of("datahub_*index_v2"));

    // Mock IOException during discovery
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Discovery failed"));

    assertThrows(IOException.class, () -> indexManager.optimizeForBulkOperations());
  }

  @Test
  public void testRestoreFromConfiguration() throws IOException {
    // First optimize settings to set up state
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock ESIndexBuilder to return ReindexConfig objects
    ReindexConfig mockConfig = mock(ReindexConfig.class);
    when(mockConfig.name()).thenReturn("datahub_dataset_v2");
    Map<String, Object> targetSettings =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "3s", ESIndexBuilder.NUMBER_OF_REPLICAS, 1));
    when(mockConfig.targetSettings()).thenReturn(targetSettings);
    when(mockIndexBuilder.buildReindexState(any(String.class), any(Map.class), any(Map.class)))
        .thenReturn(mockConfig);

    // Mock update settings
    when(mockSearchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(null);

    // Optimize settings first
    indexManager.optimizeForBulkOperations();
    assertTrue(indexManager.isSettingsOptimized());

    // Now test restore
    indexManager.restoreFromConfiguration();

    assertFalse(indexManager.isSettingsOptimized());
  }

  @Test
  public void testRestoreFromConfigurationWithIOException() throws IOException {
    // Set up state first
    GetIndexResponse mockResponse = mock(GetIndexResponse.class);
    String[] allIndices = {"datahub_dataset_v2"};
    when(mockResponse.getIndices()).thenReturn(allIndices);
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockResponse);

    // Mock ESIndexBuilder to return ReindexConfig objects
    ReindexConfig mockConfig = mock(ReindexConfig.class);
    when(mockConfig.name()).thenReturn("datahub_dataset_v2");
    Map<String, Object> targetSettings =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "3s", ESIndexBuilder.NUMBER_OF_REPLICAS, 1));
    when(mockConfig.targetSettings()).thenReturn(targetSettings);
    when(mockIndexBuilder.buildReindexState(any(String.class), any(Map.class), any(Map.class)))
        .thenReturn(mockConfig);

    // Mock ESIndexBuilder methods to throw IOException during restore
    doNothing()
        .when(mockIndexBuilder)
        .setIndexRefreshInterval(any(String.class), any(String.class));
    doNothing().when(mockIndexBuilder).tweakReplicas(any(ReindexConfig.class), any(Boolean.class));
    doThrow(new IOException("Update failed"))
        .when(mockIndexBuilder)
        .setIndexReplicaCount(any(String.class), any(Integer.class));

    // Optimize settings first
    indexManager.optimizeForBulkOperations();
    assertTrue(indexManager.isSettingsOptimized());

    // Now test restore
    assertThrows(RuntimeException.class, () -> indexManager.restoreFromConfiguration());
  }

  @Test
  public void testOptimizeForBulkOperationsManagesBothRefreshAndReplicas() throws IOException {
    // Mock responses for index discovery
    GetIndexResponse mockEntityResponse = mock(GetIndexResponse.class);
    String[] entityIndices = {"datahub_datasetindex_v2"};
    when(mockEntityResponse.getIndices()).thenReturn(entityIndices);

    GetIndexResponse mockGraphResponse = mock(GetIndexResponse.class);
    GetIndexResponse mockSystemMetadataResponse = mock(GetIndexResponse.class);
    when(mockGraphResponse.getIndices()).thenReturn(new String[0]);
    when(mockSystemMetadataResponse.getIndices()).thenReturn(new String[0]);

    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockEntityResponse)
        .thenReturn(mockGraphResponse)
        .thenReturn(mockSystemMetadataResponse);

    // Mock index convention patterns
    when(mockIndexConvention.getAllEntityIndicesPatterns())
        .thenReturn(List.of("datahub_*index_v2"));
    when(mockIndexConvention.getIndexName(ElasticSearchGraphService.INDEX_NAME))
        .thenReturn("datahub_graph_service_v1");
    when(mockIndexConvention.getIndexName(ElasticSearchSystemMetadataService.INDEX_NAME))
        .thenReturn("datahub_system_metadata_service_v1");

    // Mock ESIndexBuilder to return ReindexConfig objects
    ReindexConfig mockConfig = mock(ReindexConfig.class);
    when(mockConfig.name()).thenReturn("datahub_datasetindex_v2");
    Map<String, Object> targetSettings =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "3s", ESIndexBuilder.NUMBER_OF_REPLICAS, 1));
    when(mockConfig.targetSettings()).thenReturn(targetSettings);
    when(mockIndexBuilder.buildReindexState(any(String.class), any(Map.class), any(Map.class)))
        .thenReturn(mockConfig);

    // Mock ESIndexBuilder methods instead of SearchClientShim
    doNothing()
        .when(mockIndexBuilder)
        .setIndexRefreshInterval(any(String.class), any(String.class));
    doNothing().when(mockIndexBuilder).tweakReplicas(any(ReindexConfig.class), any(Boolean.class));

    indexManager.optimizeForBulkOperations();

    assertTrue(indexManager.isSettingsOptimized());

    // Verify that ESIndexBuilder methods were called
    verify(mockIndexBuilder, times(1)).setIndexRefreshInterval("datahub_datasetindex_v2", "-1");
    verify(mockIndexBuilder, times(1)).tweakReplicas(mockConfig, false);
  }

  @Test
  public void testRestoreFromConfigurationWithPerIndexOverrides() throws IOException {
    // Mock entity indices response
    GetIndexResponse mockEntityResponse = mock(GetIndexResponse.class);
    String[] entityIndices = {"datahub_dataset_v2", "datahub_dashboard_v2"};
    when(mockEntityResponse.getIndices()).thenReturn(entityIndices);

    // Mock graph service index response (empty)
    GetIndexResponse mockGraphResponse = mock(GetIndexResponse.class);
    when(mockGraphResponse.getIndices()).thenReturn(new String[0]);

    // Mock system metadata index response (empty)
    GetIndexResponse mockSystemMetadataResponse = mock(GetIndexResponse.class);
    when(mockSystemMetadataResponse.getIndices()).thenReturn(new String[0]);

    // Mock the search client to return different responses for different patterns
    when(mockSearchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(mockEntityResponse)
        .thenReturn(mockGraphResponse)
        .thenReturn(mockSystemMetadataResponse);

    // Mock index convention patterns
    when(mockIndexConvention.getAllEntityIndicesPatterns())
        .thenReturn(List.of("datahub_*index_v2"));
    when(mockIndexConvention.getIndexName(ElasticSearchGraphService.INDEX_NAME))
        .thenReturn("datahub_graph_service_v1");
    when(mockIndexConvention.getIndexName(ElasticSearchSystemMetadataService.INDEX_NAME))
        .thenReturn("datahub_system_metadata_service_v1");

    // Mock ESIndexBuilder to return ReindexConfig with different per-index settings
    ReindexConfig mockConfig1 = mock(ReindexConfig.class);
    ReindexConfig mockConfig2 = mock(ReindexConfig.class);

    when(mockConfig1.name()).thenReturn("datahub_dataset_v2");
    when(mockConfig2.name()).thenReturn("datahub_dashboard_v2");

    // Mock different target settings for each index (simulating per-index overrides)
    Map<String, Object> targetSettings1 =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "5s", ESIndexBuilder.NUMBER_OF_REPLICAS, 2));
    Map<String, Object> targetSettings2 =
        Map.of(
            "index",
            Map.of(ESIndexBuilder.REFRESH_INTERVAL, "10s", ESIndexBuilder.NUMBER_OF_REPLICAS, 0));

    when(mockConfig1.targetSettings()).thenReturn(targetSettings1);
    when(mockConfig2.targetSettings()).thenReturn(targetSettings2);

    when(mockIndexBuilder.buildReindexState(any(String.class), any(Map.class), any(Map.class)))
        .thenReturn(mockConfig1)
        .thenReturn(mockConfig2);

    // Mock ESIndexBuilder methods
    doNothing()
        .when(mockIndexBuilder)
        .setIndexRefreshInterval(any(String.class), any(String.class));
    doNothing().when(mockIndexBuilder).setIndexReplicaCount(any(String.class), any(Integer.class));
    doNothing().when(mockIndexBuilder).tweakReplicas(any(ReindexConfig.class), any(Boolean.class));

    // First optimize settings
    indexManager.optimizeForBulkOperations();
    assertTrue(indexManager.isSettingsOptimized());

    // Now test restore with per-index overrides
    indexManager.restoreFromConfiguration();

    assertFalse(indexManager.isSettingsOptimized());

    // Verify that ESIndexBuilder methods were called for both indices with their specific settings
    verify(mockIndexBuilder, times(1)).setIndexRefreshInterval("datahub_dataset_v2", "5s");
    verify(mockIndexBuilder, times(1)).setIndexReplicaCount("datahub_dataset_v2", 2);
    verify(mockIndexBuilder, times(1)).setIndexRefreshInterval("datahub_dashboard_v2", "10s");
    verify(mockIndexBuilder, times(1)).setIndexReplicaCount("datahub_dashboard_v2", 0);
  }
}
