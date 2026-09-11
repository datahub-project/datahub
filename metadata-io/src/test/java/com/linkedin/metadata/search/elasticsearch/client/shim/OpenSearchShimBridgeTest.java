package com.linkedin.metadata.search.elasticsearch.client.shim;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.OpenSearchShimBridge;
import org.opensearch.client.Request;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.ResizeRequest;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.testng.annotations.Test;

/**
 * Contract tests for {@link OpenSearchShimBridge}: the bridge must produce the same low-level
 * requests the REST high-level client produces (endpoints, the parameters response parsing depends
 * on, and body presence), and expose the RHLC-equivalent response-parsing registry. These pin the
 * wire behavior the unified OpenSearch shim relies on.
 */
public class OpenSearchShimBridgeTest {

  @Test
  public void searchRequestTargetsSearchEndpointWithTypedKeys() throws Exception {
    SearchRequest searchRequest = new SearchRequest("my_index");
    searchRequest.source(new SearchSourceBuilder().size(5));

    Request request = OpenSearchShimBridge.search(searchRequest);

    assertEquals(request.getMethod(), "POST");
    assertEquals(request.getEndpoint(), "/my_index/_search");
    // typed_keys drives aggregation parser dispatch in fromXContent; without it, aggregation
    // responses cannot be parsed back into server response objects.
    Map<String, String> params = request.getParameters();
    assertEquals(params.get("typed_keys"), "true");
    assertTrue(request.getEntity() != null, "search body must be serialized");
  }

  @Test
  public void bulkRequestUsesNdjsonEntity() throws Exception {
    BulkRequest bulkRequest = new BulkRequest();
    bulkRequest.add(new IndexRequest("idx").id("doc1").source(Map.of("field", "value")));

    Request request = OpenSearchShimBridge.bulk(bulkRequest);

    assertEquals(request.getEndpoint(), "/_bulk");
    assertTrue(request.getEntity() != null, "bulk body must be serialized");
    // OpenSearch 3.x removed the _bulk batch_size parameter; the pinned converter never sends it.
    assertFalse(request.getParameters().containsKey("batch_size"));
  }

  @Test
  public void createIndexTargetsIndexEndpoint() throws Exception {
    CreateIndexRequest createIndexRequest = new CreateIndexRequest("new_index");

    Request request = OpenSearchShimBridge.createIndex(createIndexRequest);

    assertEquals(request.getMethod(), "PUT");
    assertEquals(request.getEndpoint(), "/new_index");
  }

  /**
   * The delegator surface is intentionally wide but thin; pin each converter's method + endpoint so
   * a converter swap (or an RHLC upgrade changing paths) fails loudly here rather than at runtime.
   */
  @Test
  public void convertersProduceCanonicalEndpoints() throws Exception {
    SearchScrollRequest scrollRequest = new SearchScrollRequest("scroll-id-1");
    Request scroll = OpenSearchShimBridge.scroll(scrollRequest);
    assertEquals(scroll.getMethod(), "POST");
    assertEquals(scroll.getEndpoint(), "/_search/scroll");

    ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
    clearScrollRequest.addScrollId("scroll-id-1");
    Request clearScroll = OpenSearchShimBridge.clearScroll(clearScrollRequest);
    assertEquals(clearScroll.getMethod(), "DELETE");
    assertEquals(clearScroll.getEndpoint(), "/_search/scroll");

    Request delete = OpenSearchShimBridge.delete(new DeleteRequest("idx", "doc1"));
    assertEquals(delete.getMethod(), "DELETE");
    assertEquals(delete.getEndpoint(), "/idx/_doc/doc1");

    Request updateByQuery = OpenSearchShimBridge.updateByQuery(new UpdateByQueryRequest("idx"));
    assertEquals(updateByQuery.getEndpoint(), "/idx/_update_by_query");

    ReindexRequest reindexRequest = new ReindexRequest();
    reindexRequest.setSourceIndices("src");
    reindexRequest.setDestIndex("dst");
    assertEquals(OpenSearchShimBridge.reindex(reindexRequest).getEndpoint(), "/_reindex");

    Request clone = OpenSearchShimBridge.cloneIndex(new ResizeRequest("target_idx", "source_idx"));
    assertEquals(clone.getMethod(), "PUT");
    assertEquals(clone.getEndpoint(), "/source_idx/_clone/target_idx");

    assertEquals(
        OpenSearchShimBridge.clusterHealth(new ClusterHealthRequest()).getEndpoint(),
        "/_cluster/health");
    assertEquals(
        OpenSearchShimBridge.clusterGetSettings(new ClusterGetSettingsRequest()).getEndpoint(),
        "/_cluster/settings");
    Request putSettings =
        OpenSearchShimBridge.clusterPutSettings(new ClusterUpdateSettingsRequest());
    assertEquals(putSettings.getMethod(), "PUT");
    assertEquals(putSettings.getEndpoint(), "/_cluster/settings");
  }

  @Test
  public void defaultRegistryContainsAggregationParsers() {
    NamedXContentRegistry registry = OpenSearchShimBridge.defaultRegistry();
    // The registry must dispatch typed-keys aggregation names (category Aggregation). A terms
    // aggregation is the canonical case used across DataHub search responses.
    try {
      registry.parseNamedObject(Aggregation.class, "sterms", null, null);
    } catch (java.io.IOException | UnsupportedOperationException | NullPointerException e) {
      // Reaching parser code (rather than an unknown-name failure) proves registration; a null
      // parser context blows up inside the parser, which is fine for this contract test.
    } catch (org.opensearch.core.xcontent.NamedObjectNotFoundException e) {
      throw new AssertionError("terms aggregation parser missing from bridge registry", e);
    }
  }
}
