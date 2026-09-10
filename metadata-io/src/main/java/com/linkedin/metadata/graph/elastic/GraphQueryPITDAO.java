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
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.slice.SliceBuilder;

/** A search DAO for Elasticsearch backend. */
@Slf4j
public class GraphQueryPITDAO extends GraphQueryBaseDAO {

  @Getter private final SearchClientShim<?> client;

  final ExecutorService pitExecutor;

  public GraphQueryPITDAO(
      SearchClientShim<?> client,
      GraphServiceConfiguration graphServiceConfig,
      ElasticSearchConfiguration config,
      MetricUtils metricUtils) {
    super(graphServiceConfig, config, metricUtils);
    this.client = client;

    // Create dedicated thread pool for PIT operations
    int maxThreads = config.getSearch().getGraph().getMaxThreads();
    this.pitExecutor =
        new ThreadPoolExecutor(
            maxThreads, // core pool size
            maxThreads, // maximum pool size
            60L,
            TimeUnit.SECONDS, // keep alive time
            new LinkedBlockingQueue<>(maxThreads), // bounded queue for backpressure
            r -> {
              Thread t = new Thread(r, "pit-worker-" + System.currentTimeMillis());
              t.setDaemon(true);
              return t;
            },
            new ThreadPoolExecutor.CallerRunsPolicy() // backpressure: caller runs when queue full
            );

    log.info("Initialized PIT thread pool with {} threads and bounded queue", maxThreads);
  }

  /** Shutdown the PIT executor service gracefully. */
  public void shutdown() {
    if (pitExecutor != null && !pitExecutor.isShutdown()) {
      log.info("Shutting down PIT thread pool");
      pitExecutor.shutdown();
      try {
        if (!pitExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          log.warn("PIT thread pool did not terminate gracefully, forcing shutdown");
          pitExecutor.shutdownNow();
          if (!pitExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
            log.error("PIT thread pool did not terminate after forced shutdown");
          }
        }
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for PIT thread pool shutdown", e);
        pitExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Search using PIT and slice-based parallel processing for better performance. Note:
   * Elasticsearch 7 doesn't support slicing with PIT searches, so we fall back to scroll+slice for
   * Elasticsearch.
   *
   * @param maxRelations The remaining capacity for relationships (decremented from original limit)
   * @param allowPartialResults If true, return partial results on timeout or maxRelations instead
   *     of throwing
   */
  @Override
  protected LineageSliceFetchResult searchWithSlices(
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
      Set<Urn> entityUrns,
      boolean allowPartialResults) {

    // Create slice-based search requests
    String pitId = null;
    String keepAlive = config.getSearch().getGraph().getImpact().getKeepAlive();
    List<CompletableFuture<List<LineageRelationship>>> sliceFutures = new ArrayList<>();
    try {
      pitId =
          ESUtils.computePointInTime(
              opContext,
              null,
              keepAlive,
              client,
              opContext
                  .getSearchContext()
                  .getIndexConvention()
                  .getIndexName(opContext, INDEX_NAME));
      final String tempPitId = pitId;

      // One budget shared across all slices of this hop (see GraphQueryBaseDAO); null == unlimited.
      final AtomicInteger sharedRemaining = newSharedRelationshipBudget(maxRelations);

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
                      sharedRemaining,
                      defaultPageSize,
                      currentSliceId,
                      slices,
                      remainingTime,
                      entityUrns,
                      allowPartialResults,
                      tempPitId,
                      keepAlive);
                },
                pitExecutor); // Use dedicated thread pool with CallerRunsPolicy for backpressure
        sliceFutures.add(sliceFuture);
      }

      // Reuse the common slice coordination logic. If the shared budget ended exhausted, the hop
      // was truncated at maxRelations — report partial explicitly, since the outer unique-entity
      // limit check can miss it when cross-slice duplicates merge away.
      return markPartialIfSharedBudgetExhausted(
          processSliceFutures(sliceFutures, remainingTime, allowPartialResults),
          sharedRemaining,
          allowPartialResults);
    } finally {
      // Cancel any still-running slice futures, then wait (bounded, see GraphQueryConstants) before
      // deleting the shared PIT. cancel(true) only sends an interrupt — without waiting, slices
      // blocked inside client.search() may still be executing against a deleted PIT.
      cancelAndDrainSliceFutures(sliceFutures);
      ESUtils.cleanupPointInTime(opContext, client, pitId, "lineage search: " + entityUrns);
    }
  }

  /**
   * Search a single slice of the data using PIT and search_after for pagination.
   *
   * @param maxRelations The remaining capacity for relationships (decremented from original limit)
   * @param allowPartialResults If true, return partial results on timeout or maxRelations instead
   *     of throwing
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
      @Nullable AtomicInteger sharedRemaining, // budget shared across this hop's slices; null=∞
      int defaultPageSize,
      int sliceId,
      int totalSlices,
      long remainingTime,
      Set<Urn> entityUrns,
      boolean allowPartialResults,
      String pitId,
      String keepAlive) {

    List<LineageRelationship> sliceRelationships = new ArrayList<>();
    Object[] searchAfter = null;
    long deadline = System.currentTimeMillis() + remainingTime;

    try {
      // Fixed page size (capped to maxRelations so a small limit does not over-fetch). Retention is
      // bounded per page below by reserving the ACTUAL retained count from the budget shared across
      // this hop's slices and truncating any tail past it, so the slices can never collectively
      // retain more than maxRelations. Reserving a full page BEFORE fetching would instead let the
      // first slice speculatively grab the whole budget and starve its siblings.
      int pageSize = maxRelations > 0 ? Math.min(defaultPageSize, maxRelations) : defaultPageSize;
      while (true) {
        // Check for thread interruption (from future.cancel(true))
        if (Thread.currentThread().isInterrupted()) {
          log.warn("Slice {} was interrupted, cleaning up PIT and stopping", sliceId);
          throw new RuntimeException("Slice " + sliceId + " was interrupted");
        }

        // Check timeout before processing
        if (System.currentTimeMillis() >= deadline) {
          log.warn("Slice {} timed out, stopping PIT search", sliceId);
          break;
        }

        // Stop before fetching once the shared per-hop budget is exhausted (consumed by this or
        // another slice): strict mode rejects, partial mode stops and searchWithSlices marks the
        // hop partial. Retention is already bounded by the reservation below.
        if (stopSliceIfSharedBudgetExhausted(
            sharedRemaining, maxRelations, sliceId, allowPartialResults)) {
          break;
        }

        // Build search request with PIT and slice configuration
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        searchSourceBuilder.size(pageSize);

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
                    return client.search(opContext, searchRequest, RequestOptions.DEFAULT);
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

        int before = sliceRelationships.size();
        sliceRelationships.addAll(pageRelationships);

        // Bound retained relationships to this slice's atomic share of the hop's shared budget.
        if (reserveOrTruncateToSharedBudget(
            sliceRelationships,
            before,
            sharedRemaining,
            maxRelations,
            sliceId,
            allowPartialResults)) {
          break; // shared budget exhausted; partial results
        }

        // Get search_after for next page
        SearchHit[] hits = response.getHits().getHits();
        if (hits.length > 0) {
          searchAfter = hits[hits.length - 1].getSortValues();
        } else {
          break;
        }
      }
    } catch (Exception e) {
      log.error("Failed to execute PIT search for slice {}", sliceId, e);
      throw new RuntimeException("Failed to execute PIT search for slice " + sliceId, e);
    }

    return sliceRelationships;
  }
}
