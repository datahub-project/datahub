package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.datahub.context.OperationFingerprint;
import java.util.Optional;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.opensearch.client.RequestOptions;
import org.testng.annotations.Test;

/** The ES8 client throws its own ResponseException; getRawTask must map its 404 itself. */
public class Es8SearchClientShimRawTaskTest {

  private static Es8SearchClientShim shimWith(RestClient restClient) {
    return Es8SearchClientShim.forTest(
        new ElasticsearchClient(new RestClientTransport(restClient, new JacksonJsonpMapper())));
  }

  private static ResponseException esResponseException(int status) throws Exception {
    Response response = mock(Response.class, RETURNS_DEEP_STUBS);
    when(response.getStatusLine().getStatusCode()).thenReturn(status);
    when(response.getEntity()).thenReturn(null);
    return new ResponseException(response);
  }

  @Test
  public void getRawTaskReturnsTaskJson() throws Exception {
    RestClient restClient = mock(RestClient.class);
    Response ok = mock(Response.class, RETURNS_DEEP_STUBS);
    String body = "{\"completed\":true}";
    when(ok.getEntity()).thenReturn(new StringEntity(body, ContentType.APPLICATION_JSON));
    when(restClient.performRequest(any(Request.class))).thenReturn(ok);

    assertEquals(
        shimWith(restClient).getRawTask(OperationFingerprint.EMPTY, "n:1", RequestOptions.DEFAULT),
        Optional.of(body));
  }

  @Test
  public void getRawTaskMaps404ToEmpty() throws Exception {
    RestClient restClient = mock(RestClient.class);
    ResponseException notFound = esResponseException(404);
    when(restClient.performRequest(any(Request.class))).thenThrow(notFound);

    assertTrue(
        shimWith(restClient)
            .getRawTask(OperationFingerprint.EMPTY, "n:1", RequestOptions.DEFAULT)
            .isEmpty());
  }

  @Test
  public void getRawTaskRethrowsOtherErrors() throws Exception {
    RestClient restClient = mock(RestClient.class);
    ResponseException serverError = esResponseException(500);
    when(restClient.performRequest(any(Request.class))).thenThrow(serverError);

    assertThrows(
        ResponseException.class,
        () ->
            shimWith(restClient)
                .getRawTask(OperationFingerprint.EMPTY, "n:1", RequestOptions.DEFAULT));
  }
}
