package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.http.client.config.RequestConfig;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.testng.annotations.Test;

public class ElasticsearchRestClientAdapterTest {

  @Test
  public void testToElasticsearchRequestCarriesOptions() {
    Request request = new Request("GET", "/_tasks/node:1");
    request.addParameter("detailed", "true");
    request.setOptions(
        RequestOptions.DEFAULT.toBuilder()
            .setRequestConfig(
                RequestConfig.custom().setConnectTimeout(5000).setSocketTimeout(10000).build())
            .addHeader("X-Opaque-Id", "diagnostics"));

    org.elasticsearch.client.Request esRequest =
        ElasticsearchRestClientAdapter.toElasticsearchRequest(new OpenSearchRestRequest(request));

    assertEquals(esRequest.getMethod(), "GET");
    assertEquals(esRequest.getEndpoint(), "/_tasks/node:1");
    assertEquals(esRequest.getParameters().get("detailed"), "true");
    RequestConfig config = esRequest.getOptions().getRequestConfig();
    assertNotNull(config);
    assertEquals(config.getConnectTimeout(), 5000);
    assertEquals(config.getSocketTimeout(), 10000);
    assertEquals(esRequest.getOptions().getHeaders().get(0).getValue(), "diagnostics");
  }
}
