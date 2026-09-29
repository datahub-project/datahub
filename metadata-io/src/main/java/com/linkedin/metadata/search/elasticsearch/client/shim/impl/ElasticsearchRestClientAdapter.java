package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import java.io.IOException;
import org.apache.http.Header;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

/**
 * Maps OpenSearch shim types to Elasticsearch REST client types. Uses {@link OpenSearchRestRequest}
 * / {@link OpenSearchRestRequestOptions} so this class can import Elasticsearch {@link Request},
 * {@link Response}, and {@link RequestOptions} without colliding with OpenSearch shim types of the
 * same simple name.
 */
public final class ElasticsearchRestClientAdapter {

  private ElasticsearchRestClientAdapter() {}

  public static Request toElasticsearchRequest(OpenSearchRestRequest request) {
    var os = request.request();
    Request esRequest = new Request(os.getMethod(), os.getEndpoint());
    esRequest.addParameters(os.getParameters());
    esRequest.setEntity(os.getEntity());
    return esRequest;
  }

  public static RequestOptions toElasticsearchRequestOptions(OpenSearchRestRequestOptions options) {
    var os = options.options();
    RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
    if (os.getRequestConfig() != null) {
      builder.setRequestConfig(os.getRequestConfig());
    }
    for (Header header : os.getHeaders()) {
      builder.addHeader(header.getName(), header.getValue());
    }
    os.getParameters().forEach(builder::addParameter);
    return builder.build();
  }

  public static Response performRequest(RestClient restClient, OpenSearchRestRequest request)
      throws IOException {
    return restClient.performRequest(toElasticsearchRequest(request));
  }
}
