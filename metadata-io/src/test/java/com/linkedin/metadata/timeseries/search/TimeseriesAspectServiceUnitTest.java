package com.linkedin.metadata.timeseries.search;

import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.TimeseriesAspectIndexBuilders;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test using mocks instead of integration for testing functionality not dependent on a real server
 * response
 */
public class TimeseriesAspectServiceUnitTest {

  private final RestHighLevelClient _searchClient = mock(RestHighLevelClient.class);
  private final IndexConvention _indexConvention = mock(IndexConvention.class);
  private final TimeseriesAspectIndexBuilders _timeseriesAspectIndexBuilders =
      mock(TimeseriesAspectIndexBuilders.class);
  private final ESBulkProcessor _bulkProcessor = mock(ESBulkProcessor.class);
  private final RestClient _restClient = mock(RestClient.class);
  private final TimeseriesAspectService _timeseriesAspectService =
      new ElasticSearchTimeseriesAspectService(
          _searchClient,
          _timeseriesAspectIndexBuilders,
          _bulkProcessor,
          0,
          QueryFilterRewriteChain.EMPTY,
          TimeseriesAspectServiceConfig.builder().build());
  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization(_indexConvention);

  private static final String INDEX_PATTERN = "indexPattern";

  @Test
  public void testGetIndicesIntegerWrap() throws IOException {
    when(_indexConvention.getAllTimeseriesAspectIndicesPattern()).thenReturn(INDEX_PATTERN);
    when(_searchClient.getLowLevelClient()).thenReturn(_restClient);
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
    when(_restClient.performRequest(any(Request.class))).thenReturn(response);

    List<TimeseriesIndexSizeResult> results = _timeseriesAspectService.getIndexSizes(opContext);

    Assert.assertEquals(results.get(0).getSizeInMb(), 8078.398031);
  }
}
