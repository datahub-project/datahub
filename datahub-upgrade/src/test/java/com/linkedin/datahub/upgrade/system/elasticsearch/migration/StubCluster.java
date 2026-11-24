package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpVersion;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;

/**
 * A cluster that answers raw requests from registered rules, so a real {@link ClusterHttp} can be
 * driven in unit tests and every request it made can be inspected afterwards.
 */
final class StubCluster {
  record Call(String method, String path, String query, String body, String opaqueId) {}

  record Reply(int status, String json) {
    static Reply ok(String json) {
      return new Reply(200, json);
    }
  }

  private record Rule(String method, String path, Function<Call, Reply> responder) {}

  final SearchClientShim<?> shim = mock(SearchClientShim.class);
  final List<Call> calls = new ArrayList<>();
  final ObjectMapper mapper = new ObjectMapper();
  private final List<Rule> rules = new ArrayList<>();

  StubCluster(SearchClientShim.SearchEngineType engine) throws IOException {
    when(shim.getEngineType()).thenReturn(engine);
    when(shim.performLowLevelRequest(any(), any(Request.class)))
        .thenAnswer(invocation -> dispatch(invocation.getArgument(1)));
  }

  OperationContext opContext() {
    OperationContext opContext = mock(OperationContext.class);
    when(opContext.getObjectMapper()).thenReturn(mapper);
    return opContext;
  }

  ClusterHttp http() {
    return new ClusterHttp(opContext(), shim);
  }

  void on(String method, String path, Reply reply) {
    rules.add(new Rule(method, path, call -> reply));
  }

  void on(String method, String path, Function<Call, Reply> responder) {
    rules.add(new Rule(method, path, responder));
  }

  List<Call> calls(String method, String path) {
    return calls.stream().filter(c -> c.method().equals(method) && c.path().equals(path)).toList();
  }

  private RawResponse dispatch(Request request) throws IOException {
    String endpoint = request.getEndpoint();
    String path = endpoint;
    String query = "";
    int q = endpoint.indexOf('?');
    if (q >= 0) {
      path = endpoint.substring(0, q);
      query = endpoint.substring(q + 1);
    }
    // Decoded as UTF-8 whatever charset the entity declares, which is what a cluster does with the
    // bytes on the wire; a body encoded any other way shows up here as the garbage it would be.
    String body =
        request.getEntity() == null ? "" : utf8(EntityUtils.toByteArray(request.getEntity()));
    String opaqueId = null;
    if (request.getOptions() != null) {
      for (Header header : request.getOptions().getHeaders()) {
        if ("X-Opaque-Id".equals(header.getName())) {
          opaqueId = header.getValue();
        }
      }
    }
    Call call = new Call(request.getMethod(), path, query, body, opaqueId);
    calls.add(call);
    for (Rule rule : rules) {
      if (rule.method().equals(call.method()) && rule.path().equals(call.path())) {
        return response(request, rule.responder().apply(call));
      }
    }
    return response(
        request, new Reply(404, "{\"error\":\"no stub for " + call.method() + " " + path + "\"}"));
  }

  private static RawResponse response(Request request, Reply reply) {
    return new RawResponse(
        new BasicRequestLine(request.getMethod(), request.getEndpoint(), HttpVersion.HTTP_1_1),
        new HttpHost("stub"),
        new NStringEntity(reply.json(), ContentType.APPLICATION_JSON),
        new BasicStatusLine(HttpVersion.HTTP_1_1, reply.status(), ""));
  }

  static String utf8(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
