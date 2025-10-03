package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.elastic.utils.GraphQueryConstants;
import com.linkedin.metadata.graph.elastic.utils.GraphQueryUtils;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.slice.SliceBuilder;

/** A search DAO for Elasticsearch backend. */
@Slf4j
public class GraphQueryOpenSearchDAO extends GraphQueryBaseDAO {

  @Getter private final RestHighLevelClient client;

  public GraphQueryOpenSearchDAO(
      RestHighLevelClient client,
      GraphServiceConfiguration graphServiceConfig,
      ElasticSearchConfiguration config,
      MetricUtils metricUtils) {
    super(graphServiceConfig, config, metricUtils);
    this.client = client;
  }

  /**
   * Search using PIT and slice-based parallel processing for better performance. Note:
   * Elasticsearch doesn't support slicing with PIT searches, so we fall back to scroll+slice for
   * Elasticsearch.
   *
   * @param maxRelations The remaining capacity for relationships (decremented from original limit)
   */
  @Override
  protected List<LineageRelationship> searchWithSlices(
      @Nonnull OperationContext opContext,
      @Nonnull QueryBuilder query,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      ThreadSafePathStore existingPaths,
      int maxRelations, // This is the REMAINING capacity, not the original total limit
      int defaultPageSize,
      int slices,
      long remainingTime,
      Set<Urn> entityUrns) {

    // Create slice-based search requests
    List<CompletableFuture<List<LineageRelationship>>> sliceFutures = new ArrayList<>();

    for (int sliceId = 0; sliceId < slices; sliceId++) {
      final int currentSliceId = sliceId;
      CompletableFuture<List<LineageRelationship>> sliceFuture =
          CompletableFuture.supplyAsync(
              () -> {
                return searchSingleSliceWithPit(
                    opContext,
                    query,
                    lineageGraphFilters,
                    visitedEntities,
                    viaEntities,
                    numHops,
                    remainingHops,
                    existingPaths,
                    maxRelations,
                    defaultPageSize,
                    currentSliceId,
                    slices,
                    remainingTime,
                    entityUrns);
              });
      sliceFutures.add(sliceFuture);
    }

    // Reuse the common slice coordination logic
    return processSliceFutures(sliceFutures, remainingTime);
  }

  /**
   * Search a single slice of the data using PIT and search_after for pagination.
   *
   * @param maxRelations The remaining capacity for relationships (decremented from original limit)
   */
  private List<LineageRelationship> searchSingleSliceWithPit(
      @Nonnull OperationContext opContext,
      @Nonnull QueryBuilder query,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      ThreadSafePathStore existingPaths,
      int maxRelations, // This is the REMAINING capacity, not the original total limit
      int defaultPageSize,
      int sliceId,
      int totalSlices,
      long remainingTime,
      Set<Urn> entityUrns) {

    List<LineageRelationship> sliceRelationships = new ArrayList<>();
    String pitId = null;
    Object[] searchAfter = null;
    String keepAlive = config.getSearch().getGraph().getImpact().getKeepAlive();

    try {
      // Create initial PIT using existing utility method
      pitId =
          ESUtils.computePointInTime(
              null,
              keepAlive,
              config.getImplementation(),
              client,
              opContext.getSearchContext().getIndexConvention().getIndexName(INDEX_NAME));

      while (sliceRelationships.size() < maxRelations) {
        // Check timeout before processing
        if (remainingTime <= 0) {
          log.warn("Slice {} timed out, stopping PIT search", sliceId);
          break;
        }

        // Build search request with PIT and slice configuration
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        searchSourceBuilder.size(defaultPageSize);

        // Add sorting for consistent results and search_after using Edge sort fields
        ESUtils.buildSortOrder(searchSourceBuilder, Edge.EDGE_SORT_CRITERION, List.of(), false);

        // Add slice configuration for parallel processing
        searchSourceBuilder.slice(new SliceBuilder(sliceId, totalSlices));

        // Set PIT and search_after
        ESUtils.setSearchAfter(searchSourceBuilder, searchAfter, pitId, keepAlive);

        searchRequest.source(searchSourceBuilder);

        // Execute search
        SearchResponse response =
            opContext.withSpan(
                "esQuery",
                () -> {
                  try {
                    if (metricUtils != null)
                      metricUtils.increment(
                          this.getClass(), GraphQueryConstants.SEARCH_EXECUTIONS_METRIC, 1);
                    return client.search(searchRequest, RequestOptions.DEFAULT);
                  } catch (Exception e) {
                    log.error("Search query failed", e);
                    throw new ESQueryException("Search query failed:", e);
                  }
                },
                MetricUtils.DROPWIZARD_NAME,
                MetricUtils.name(this.getClass(), "esQuery"));

        if (response == null
            || response.getHits() == null
            || response.getHits().getHits().length == 0) {
          log.debug("Slice {} completed, no more results", sliceId);
          break;
        }

        // Extract relationships from current page
        List<LineageRelationship> pageRelationships =
            GraphQueryUtils.extractRelationships(
                entityUrns,
                response,
                lineageGraphFilters,
                visitedEntities,
                viaEntities,
                numHops,
                remainingHops,
                existingPaths,
                false); // exploreMultiplePaths - not needed for impact lineage

        sliceRelationships.addAll(pageRelationships);

        // Safety check to prevent exceeding the limit
        if (sliceRelationships.size() >= maxRelations) {
          log.error("Slice {} reached maxRelations limit, stopping PIT search", sliceId);
          throw new IllegalStateException(
              String.format(
                  "Slice %d exceeded maxRelations limit of %d. Consider reducing maxHops or increasing the maxRelations limit.",
                  sliceId, maxRelations));
        }

        // Get search_after for next page
        SearchHit[] hits = response.getHits().getHits();
        if (hits.length > 0) {
          searchAfter = hits[hits.length - 1].getSortValues();
        } else {
          break;
        }

        // Update remaining time
        remainingTime = System.currentTimeMillis() - (System.currentTimeMillis() - remainingTime);
      }
    } catch (Exception e) {
      log.error("Failed to execute PIT search for slice {}", sliceId, e);
      throw new RuntimeException("Failed to execute PIT search for slice " + sliceId, e);
    } finally {
      // Clean up PIT to prevent hitting the limit
      ESUtils.cleanupPointInTime(client, pitId, config.getImplementation(), "slice " + sliceId);
    }

    return sliceRelationships;
  }
}
