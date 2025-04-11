package com.linkedin.metadata.timeseries.search;

import static org.mockito.Mockito.*;

import com.datahub.util.exception.ESQueryException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.TimeseriesAspectIndexBuilders;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test using mocks instead of integration for testing functionality not dependent on a real server
 * response
 */
public class TimeseriesAspectServiceUnitTest {

  private final RestHighLevelClient searchClient = mock(RestHighLevelClient.class);
  private final IndexConvention indexConvention = mock(IndexConvention.class);
  private final TimeseriesAspectIndexBuilders timeseriesAspectIndexBuilders =
      mock(TimeseriesAspectIndexBuilders.class);
  private final ESBulkProcessor bulkProcessor = mock(ESBulkProcessor.class);
  private final RestClient restClient = mock(RestClient.class);
  private final TimeseriesAspectService _timeseriesAspectService =
      new ElasticSearchTimeseriesAspectService(
          searchClient,
          timeseriesAspectIndexBuilders,
          bulkProcessor,
          0,
          QueryFilterRewriteChain.EMPTY,
          TimeseriesAspectServiceConfig.builder().build());
  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization(indexConvention);

  private static final String INDEX_PATTERN = "indexPattern";

  @BeforeMethod
  public void resetMocks() {
    reset(searchClient, indexConvention, timeseriesAspectIndexBuilders, bulkProcessor, restClient);
  }

  @Test
  public void testGetIndicesIntegerWrap() throws IOException {
    when(indexConvention.getAllTimeseriesAspectIndicesPattern()).thenReturn(INDEX_PATTERN);
    when(searchClient.getLowLevelClient()).thenReturn(restClient);
    ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
    ObjectNode indicesNode = JsonNodeFactory.instance.objectNode();
    ObjectNode indexNode = JsonNodeFactory.instance.objectNode();
    ObjectNode primariesNode = JsonNodeFactory.instance.objectNode();
    ObjectNode storeNode = JsonNodeFactory.instance.objectNode();
    NumericNode bytesNode = JsonNodeFactory.instance.numberNode(8078398031L);
    storeNode.set("size_in_bytes", bytesNode);
    primariesNode.set("store", storeNode);
    indexNode.set("primaries", primariesNode);
    indicesNode.set("someIndexName", indexNode);
    jsonNode.set("indices", indicesNode);

    Response response = mock(Response.class);
    HttpEntity responseEntity = mock(HttpEntity.class);
    when(response.getEntity()).thenReturn(responseEntity);
    when(responseEntity.getContent())
        .thenReturn(IOUtils.toInputStream(jsonNode.toString(), StandardCharsets.UTF_8));
    when(restClient.performRequest(any(Request.class))).thenReturn(response);

    List<TimeseriesIndexSizeResult> results = _timeseriesAspectService.getIndexSizes(opContext);

    Assert.assertEquals(results.get(0).getSizeInMb(), 8078.398031);
  }

  @Test
  public void testSearchQueryFailure() throws IOException {
    // setup mock
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("testAspect")))
        .thenReturn("dataset_testAspect_index_v1");

    // Setup search request that will fail
    when(searchClient.search(any(), any())).thenThrow(new IOException("Search failed"));

    Filter filter = QueryUtils.newFilter("field", "value");

    // Verify that ESQueryException is thrown with correct message
    try {
      _timeseriesAspectService.getAspectValues(
          opContext,
          UrnUtils.getUrn("urn:li:dataset:123"),
          "dataset",
          "testAspect",
          null,
          null,
          10,
          filter,
          null);
      Assert.fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertTrue(e.getCause() instanceof IOException);
      Assert.assertEquals(e.getCause().getMessage(), "Search failed");
    }
  }

  @Test
  public void testScrollSearchQueryFailure() throws IOException {
    // setup mock
    when(indexConvention.getTimeseriesAspectIndexName(eq("dataset"), eq("testAspect")))
        .thenReturn("dataset_testAspect_index_v1");

    // Setup search request that will fail
    when(searchClient.search(any(), any())).thenThrow(new IOException("Scroll search failed"));

    Filter filter = QueryUtils.newFilter("field", "value");
    List<SortCriterion> sortCriteria =
        Arrays.asList(new SortCriterion().setField("timestamp").setOrder(SortOrder.DESCENDING));

    // Verify that ESQueryException is thrown with correct message
    try {
      _timeseriesAspectService.scrollAspects(
          opContext, "dataset", "testAspect", filter, sortCriteria, null, 10, null, null);
      Assert.fail("Expected ESQueryException to be thrown");
    } catch (ESQueryException e) {
      Assert.assertEquals(e.getMessage(), "Search query failed:");
      Assert.assertTrue(e.getCause() instanceof IOException);
      Assert.assertEquals(e.getCause().getMessage(), "Scroll search failed");
    }
  }
}
