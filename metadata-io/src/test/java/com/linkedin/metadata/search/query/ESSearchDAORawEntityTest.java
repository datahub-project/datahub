package com.linkedin.metadata.search.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.testng.annotations.Test;

public class ESSearchDAORawEntityTest {

  @Test
  public void testRawEntityWithMockedClient() throws Exception {
    // Setup mocks
    RestHighLevelClient mockClient = Mockito.mock(RestHighLevelClient.class);
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();

    // Mock search response
    SearchResponse mockResponse = Mockito.mock(SearchResponse.class);
    SearchHits mockHits = Mockito.mock(SearchHits.class);
    Mockito.when(mockHits.getHits()).thenReturn(new SearchHit[0]);
    Mockito.when(mockResponse.getHits()).thenReturn(mockHits);

    // Setup behavior for mocks
    Mockito.when(mockClient.search(Mockito.any(), Mockito.eq(RequestOptions.DEFAULT)))
        .thenReturn(mockResponse);

    // Create test URN
    Urn datasetUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)");
    Set<Urn> urns = new HashSet<>();
    urns.add(datasetUrn);

    // Create ESSearchDAO with mocked client
    ESSearchDAO esSearchDAO =
        new ESSearchDAO(
            mockClient,
            false,
            "elasticsearch",
            new SearchConfiguration(),
            null,
            com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain.EMPTY);

    // Execute rawEntity method
    Map<Urn, SearchResponse> results = esSearchDAO.rawEntity(opContext, urns);

    // Verify results
    assertNotNull(results);
    assertEquals(results.size(), 1);
    assertTrue(results.containsKey(datasetUrn));
    assertEquals(results.get(datasetUrn), mockResponse);

    // Verify the search was performed with correct parameters
    Mockito.verify(mockClient).search(Mockito.any(), Mockito.eq(RequestOptions.DEFAULT));
  }
}
