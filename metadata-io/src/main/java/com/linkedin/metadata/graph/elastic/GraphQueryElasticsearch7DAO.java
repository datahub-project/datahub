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
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.slice.SliceBuilder;

/** A search DAO for Elasticsearch backend. */
@Slf4j
public class GraphQueryElasticsearch7DAO extends GraphQueryBaseDAO {

  @Getter private final RestHighLevelClient client;

  public GraphQueryElasticsearch7DAO(
      RestHighLevelClient client,
      GraphServiceConfiguration graphServiceConfig,
      ElasticSearchConfiguration config,
      MetricUtils metricUtils) {
    super(graphServiceConfig, config, metricUtils);
    this.client = client;
  }

  /**
   * Search using scroll and slice-based parallel processing for Elasticsearch compatibility. This
   * method reuses the slice coordination logic but uses scroll instead of PIT.
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
      int maxRelations,
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
                return searchSingleSliceWithScroll(
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

    // Reuse the existing slice coordination logic
    return processSliceFutures(sliceFutures, remainingTime);
  }

  /**
   * Search a single slice of the data using scroll and slice for pagination. This is the
   * Elasticsearch-compatible alternative to searchSingleSliceWithPit.
   *
   * @param maxRelations The remaining capacity for relationships (decremented from original limit)
   */
  private List<LineageRelationship> searchSingleSliceWithScroll(
      @Nonnull OperationContext opContext,
      @Nonnull QueryBuilder query,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      ThreadSafePathStore existingPaths,
      int maxRelations,
      int defaultPageSize,
      int sliceId,
      int totalSlices,
      long remainingTime,
      Set<Urn> entityUrns) {

    List<LineageRelationship> sliceRelationships = new ArrayList<>();
    String scrollId = null;
    String keepAlive = config.getSearch().getGraph().getImpact().getKeepAlive();

    try {
      // Build initial search request with scroll and slice
      SearchRequest searchRequest = new SearchRequest();
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.query(query);
      searchSourceBuilder.size(defaultPageSize);

      // Add sorting for consistent results
      ESUtils.buildSortOrder(searchSourceBuilder, Edge.EDGE_SORT_CRITERION, List.of(), false);

      // Add slice configuration for parallel processing
      searchSourceBuilder.slice(new SliceBuilder(sliceId, totalSlices));

      // Set scroll keepAlive using configured value
      searchRequest.scroll(keepAlive);

      searchRequest.source(searchSourceBuilder);
      searchRequest.indices(
          opContext.getSearchContext().getIndexConvention().getIndexName(INDEX_NAME));

      // Execute initial search to get scroll ID
      SearchResponse response = executeSearch(opContext, searchRequest);
      scrollId = response.getScrollId();

      if (response == null
          || response.getHits() == null
          || response.getHits().getHits().length == 0) {
        log.debug("Slice {} completed, no initial results", sliceId);
        return sliceRelationships;
      }

      // Process initial results
      processSearchResponse(
          response,
          sliceRelationships,
          entityUrns,
          lineageGraphFilters,
          visitedEntities,
          viaEntities,
          numHops,
          remainingHops,
          existingPaths);

      // Continue scrolling until we reach the limit or no more results
      while (sliceRelationships.size() < maxRelations && remainingTime > 0) {
        // Check timeout
        if (remainingTime <= 0) {
          log.warn("Slice {} timed out, stopping scroll search", sliceId);
          break;
        }

        // Continue scroll
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        scrollRequest.scroll(keepAlive); // Use configured keepAlive

        response =
            opContext.withSpan(
                "esScrollQuery",
                () -> {
                  try {
                    if (metricUtils != null)
                      metricUtils.increment(
                          this.getClass(), GraphQueryConstants.SEARCH_EXECUTIONS_METRIC, 1);
                    return client.scroll(scrollRequest, RequestOptions.DEFAULT);
                  } catch (Exception e) {
                    log.error("Scroll query failed", e);
                    throw new ESQueryException("Scroll query failed:", e);
                  }
                },
                MetricUtils.DROPWIZARD_NAME,
                MetricUtils.name(this.getClass(), "esScrollQuery"));

        if (response == null
            || response.getHits() == null
            || response.getHits().getHits().length == 0) {
          log.debug("Slice {} completed, no more results", sliceId);
          break;
        }

        // Process scroll results
        processSearchResponse(
            response,
            sliceRelationships,
            entityUrns,
            lineageGraphFilters,
            visitedEntities,
            viaEntities,
            numHops,
            remainingHops,
            existingPaths);

        // Update remaining time
        remainingTime = System.currentTimeMillis() - (System.currentTimeMillis() - remainingTime);
      }

      // Safety check
      if (sliceRelationships.size() >= maxRelations) {
        log.error("Slice {} reached maxRelations limit, stopping scroll search", sliceId);
        throw new IllegalStateException(
            String.format(
                "Slice %d exceeded maxRelations limit of %d. Consider reducing maxHops or increasing the maxRelations limit.",
                sliceId, maxRelations));
      }

    } catch (Exception e) {
      log.error("Failed to execute scroll search for slice {}", sliceId, e);
      throw new RuntimeException("Failed to execute scroll search for slice " + sliceId, e);
    } finally {
      // Clean up scroll context
      if (scrollId != null) {
        try {
          ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
          clearScrollRequest.addScrollId(scrollId);
          client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
          log.warn("Failed to clear scroll context for slice {}", sliceId, e);
        }
      }
    }

    return sliceRelationships;
  }

  /** Execute a search request with common error handling and metrics. */
  private SearchResponse executeSearch(OperationContext opContext, SearchRequest searchRequest) {
    return opContext.withSpan(
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
  }

  /** Process search response and extract relationships. */
  private void processSearchResponse(
      SearchResponse response,
      List<LineageRelationship> sliceRelationships,
      Set<Urn> entityUrns,
      LineageGraphFilters lineageGraphFilters,
      Set<Urn> visitedEntities,
      Set<Urn> viaEntities,
      int numHops,
      int remainingHops,
      ThreadSafePathStore existingPaths) {

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
            false); // exploreMultiplePaths - not needed for slice-based search

    sliceRelationships.addAll(pageRelationships);
  }
}
