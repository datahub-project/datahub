package com.linkedin.metadata.graph.elastic;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.query.filter.SortCriterion;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchResponse;

/**
 * Interface for graph query DAOs that can work with different search engine implementations. This
 * provides a common contract for both OpenSearch and Elasticsearch implementations.
 */
public interface GraphQueryDAO {

  /** Get lineage response for a given entity with pagination. */
  LineageResponse getLineage(
      @Nonnull final OperationContext opContext,
      @Nonnull Urn entityUrn,
      LineageGraphFilters lineageGraphFilters,
      int offset,
      @Nullable Integer count,
      int maxHops);

  /** Get impact lineage for a given entity. */
  LineageResponse getImpactLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters filters,
      int maxHops);

  /** Get search response for graph queries with basic parameters. */
  SearchResponse getSearchResponse(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters filters,
      int offset,
      @Nullable Integer count);

  /** Get search response for graph queries with advanced parameters. */
  SearchResponse getSearchResponse(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters filters,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer count);

  void cleanupPointInTime(String pitId);
}
