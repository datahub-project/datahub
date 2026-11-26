package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ScriptField;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.opensearch.client.RequestOptions;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.Test;

public class Es8SearchClientShimScriptFieldsTest {

  @Test
  public void searchForwardsScriptFields() throws IOException {
    ElasticsearchClient mockClient = mock(ElasticsearchClient.class);
    ArgumentCaptor<SearchRequest> captor = ArgumentCaptor.forClass(SearchRequest.class);
    SearchResponse<JsonNode> empty =
        SearchResponse.of(
            r ->
                r.took(1)
                    .timedOut(false)
                    .shards(s -> s.total(1).successful(1).failed(0))
                    .hits(h -> h.hits(List.of())));
    when(mockClient.search(captor.capture(), eq(JsonNode.class))).thenReturn(empty);
    Es8SearchClientShim shim = Es8SearchClientShim.forTest(mockClient);
    String source = "return params['_source'].keySet();";

    shim.search(
        OperationFingerprint.EMPTY,
        new org.opensearch.action.search.SearchRequest()
            .source(
                new SearchSourceBuilder()
                    .scriptField(
                        "names", new Script(ScriptType.INLINE, "painless", source, Map.of()))),
        RequestOptions.DEFAULT);

    ScriptField sent = captor.getValue().scriptFields().get("names");
    assertNotNull(sent, "a script field the caller asked for reaches the engine");
    assertEquals(sent.script().source(), source);
    assertEquals(sent.script().lang(), "painless");
  }
}
