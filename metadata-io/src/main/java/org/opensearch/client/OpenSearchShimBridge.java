package org.opensearch.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.indices.AnalyzeRequest;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.client.indices.ResizeRequest;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.index.reindex.UpdateByQueryRequest;

/**
 * Same-package bridge exposing the package-private request converters (and default response parsing
 * registry) of the REST high-level client, so the unified OpenSearch shim can produce
 * wire-identical low-level {@link Request}s without routing I/O through the deprecated {@link
 * RestHighLevelClient}.
 *
 * <p>This class deliberately lives in {@code org.opensearch.client}. The RHLC jar is pinned
 * (compile and runtime lock to the same version — see the version catalog and gradle lockfiles), is
 * unsealed, and has no module-info, so the split package is safe on the classpath-only Spring Boot
 * deployments this codebase supports. Do NOT add a {@code package-info.java} for this package, do
 * not relocate/shade the RHLC jar without relocating this class identically, and keep this class
 * free of any state.
 *
 * <p>If the runtime ever moves to the JPMS module path or a multi-classloader container (e.g.
 * Spring DevTools restart loader), this bridge fails with {@code IllegalAccessError}; the fallback
 * is copying the converter sources in-tree (Apache-2.0).
 */
public final class OpenSearchShimBridge {

  private OpenSearchShimBridge() {}

  /**
   * Response-parsing registry equivalent to the one {@link RestHighLevelClient} builds: default
   * named XContent entries (aggregations, suggestions) plus {@code NamedXContentProvider}s
   * discovered via {@code ServiceLoader}.
   */
  public static NamedXContentRegistry defaultRegistry() {
    List<NamedXContentRegistry.Entry> entries =
        new ArrayList<>(RestHighLevelClient.getDefaultNamedXContents());
    entries.addAll(RestHighLevelClient.getProvidedNamedXContents());
    return new NamedXContentRegistry(entries);
  }

  // Core search operations

  public static Request search(SearchRequest request) throws IOException {
    return RequestConverters.search(request, "_search");
  }

  public static Request scroll(SearchScrollRequest request) throws IOException {
    return RequestConverters.searchScroll(request);
  }

  public static Request clearScroll(ClearScrollRequest request) throws IOException {
    return RequestConverters.clearScroll(request);
  }

  public static Request count(CountRequest request) throws IOException {
    return RequestConverters.count(request);
  }

  public static Request explain(ExplainRequest request) throws IOException {
    return RequestConverters.explain(request);
  }

  public static Request createPit(CreatePitRequest request) throws IOException {
    return RequestConverters.createPit(request);
  }

  public static Request deletePit(DeletePitRequest request) throws IOException {
    return RequestConverters.deletePit(request);
  }

  // Document operations

  public static Request get(GetRequest request) {
    return RequestConverters.get(request);
  }

  public static Request index(IndexRequest request) {
    return RequestConverters.index(request);
  }

  public static Request delete(DeleteRequest request) {
    return RequestConverters.delete(request);
  }

  public static Request bulk(BulkRequest request) throws IOException {
    return RequestConverters.bulk(request);
  }

  public static Request deleteByQuery(DeleteByQueryRequest request) throws IOException {
    return RequestConverters.deleteByQuery(request);
  }

  public static Request submitDeleteByQuery(DeleteByQueryRequest request) throws IOException {
    return RequestConverters.submitDeleteByQuery(request);
  }

  public static Request updateByQuery(UpdateByQueryRequest request) throws IOException {
    return RequestConverters.updateByQuery(request);
  }

  public static Request reindex(ReindexRequest request) throws IOException {
    return RequestConverters.reindex(request);
  }

  public static Request submitReindex(ReindexRequest request) throws IOException {
    return RequestConverters.submitReindex(request);
  }

  // Index management operations

  public static Request createIndex(CreateIndexRequest request) throws IOException {
    return IndicesRequestConverters.createIndex(request);
  }

  public static Request deleteIndex(DeleteIndexRequest request) {
    return IndicesRequestConverters.deleteIndex(request);
  }

  public static Request getIndex(GetIndexRequest request) {
    return IndicesRequestConverters.getIndex(request);
  }

  public static Request indicesExist(GetIndexRequest request) {
    return IndicesRequestConverters.indicesExist(request);
  }

  public static Request putMapping(PutMappingRequest request) throws IOException {
    return IndicesRequestConverters.putMapping(request);
  }

  public static Request getMappings(GetMappingsRequest request) {
    return IndicesRequestConverters.getMappings(request);
  }

  public static Request getSettings(GetSettingsRequest request) {
    return IndicesRequestConverters.getSettings(request);
  }

  public static Request indexPutSettings(UpdateSettingsRequest request) throws IOException {
    return IndicesRequestConverters.indexPutSettings(request);
  }

  public static Request refresh(RefreshRequest request) {
    return IndicesRequestConverters.refresh(request);
  }

  public static Request getAlias(GetAliasesRequest request) {
    return IndicesRequestConverters.getAlias(request);
  }

  public static Request updateAliases(IndicesAliasesRequest request) throws IOException {
    return IndicesRequestConverters.updateAliases(request);
  }

  public static Request analyze(AnalyzeRequest request) throws IOException {
    return IndicesRequestConverters.analyze(request);
  }

  public static Request cloneIndex(ResizeRequest request) throws IOException {
    return IndicesRequestConverters.clone(request);
  }

  // Cluster operations

  public static Request clusterHealth(ClusterHealthRequest request) {
    return ClusterRequestConverters.clusterHealth(request);
  }

  public static Request clusterGetSettings(ClusterGetSettingsRequest request) throws IOException {
    return ClusterRequestConverters.clusterGetSettings(request);
  }

  public static Request clusterPutSettings(ClusterUpdateSettingsRequest request)
      throws IOException {
    return ClusterRequestConverters.clusterPutSettings(request);
  }

  // Task operations

  public static Request listTasks(ListTasksRequest request) {
    return TasksRequestConverters.listTasks(request);
  }

  public static Request getTask(GetTaskRequest request) {
    return TasksRequestConverters.getTask(request);
  }

  // Info

  public static Request info() {
    return RequestConverters.info();
  }
}
