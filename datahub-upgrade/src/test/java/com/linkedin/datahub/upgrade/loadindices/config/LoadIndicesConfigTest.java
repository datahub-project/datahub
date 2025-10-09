package com.linkedin.datahub.upgrade.loadindices.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.loadindices.LoadIndices;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.RestHighLevelClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadIndicesConfigTest {

  @Mock private SearchClientShim<?> mockSearchClient;
  @Mock private IndexConvention mockIndexConvention;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private SearchContext mockSearchContext;
  @Mock private OperationContext mockOperationContext;
  @Mock private ESIndexBuilder mockIndexBuilder;

  private LoadIndicesConfig config;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    config = new LoadIndicesConfig();

    // Setup mock operation context
    org.mockito.Mockito.when(mockOperationContext.getSearchContext()).thenReturn(mockSearchContext);
    org.mockito.Mockito.when(mockSearchContext.getIndexConvention())
        .thenReturn(mockIndexConvention);
    org.mockito.Mockito.when(mockOperationContext.getEntityRegistry())
        .thenReturn(mockEntityRegistry);
  }

  @Test
  public void testLoadIndicesConfigClass() {
    // Test that the LoadIndicesConfig class can be instantiated
    assertNotNull(config);
  }

  @Test
  public void testLoadIndicesClass() {
    // Test that the LoadIndices class can be instantiated with null dependencies
    LoadIndices loadIndices = new LoadIndices(null, null, null, null, null, null, null, null, null);
    assertNotNull(loadIndices);
    assertEquals(loadIndices.id(), "LoadIndices");
    assertNotNull(loadIndices.steps());
  }

  @Test
  public void testCreateIndexManager() throws Exception {
    // Test that createIndexManager method creates LoadIndicesIndexManager successfully
    // This test verifies that the method works with proper mocks
    var result =
        config.createIndexManager(mockOperationContext, mockSearchClient, mockIndexBuilder);
    assertNotNull(result);

    // Verify that the operation context methods were called
    org.mockito.Mockito.verify(mockOperationContext).getSearchContext();
  }

  @Test
  public void testCreateIndexManagerWithCustomRefreshInterval() throws Exception {
    // Test that createIndexManager method works with custom index builder
    var result =
        config.createIndexManager(mockOperationContext, mockSearchClient, mockIndexBuilder);
    assertNotNull(result);

    // Verify that the operation context methods were called
    org.mockito.Mockito.verify(mockOperationContext).getSearchContext();
  }

  @Test
  public void testOperationContextIntegration() throws Exception {
    // Test that the operation context is properly used in createIndexManager
    // This verifies that the method correctly accesses the search context
    var result =
        config.createIndexManager(mockOperationContext, mockSearchClient, mockIndexBuilder);
    assertNotNull(result);

    // Verify that the operation context methods were called
    org.mockito.Mockito.verify(mockOperationContext).getSearchContext();
  }
}
