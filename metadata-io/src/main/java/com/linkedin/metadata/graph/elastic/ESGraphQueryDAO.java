package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH;
import static com.linkedin.metadata.Constants.ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RestHighLevelClient;

/** A search DAO for Elasticsearch backend. */
@Slf4j
public class ESGraphQueryDAO implements GraphQueryDAO {

  private final GraphQueryBaseDAO delegate;
  @Getter private final GraphServiceConfiguration graphServiceConfig;
  private final ElasticSearchConfiguration config;

  public ESGraphQueryDAO(
      RestHighLevelClient client,
      GraphServiceConfiguration graphServiceConfig,
      ElasticSearchConfiguration config,
      MetricUtils metricUtils) {

    this.graphServiceConfig = graphServiceConfig;
    this.config = config;

    switch (config.getImplementation()) {
      case ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH:
        this.delegate =
            new GraphQueryElasticsearch7DAO(client, graphServiceConfig, config, metricUtils);
        break;
      case ELASTICSEARCH_IMPLEMENTATION_OPENSEARCH:
        this.delegate =
            new GraphQueryOpenSearchDAO(client, graphServiceConfig, config, metricUtils);
        break;
      default:
        throw new NotImplementedException("Unsupported Elasticsearch implementation");
    }
  }

  public ElasticSearchConfiguration getESSearchConfig() {
    return config;
  }

  @Override
  public LineageResponse getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      LineageGraphFilters lineageGraphFilters,
      int offset,
      @Nullable Integer count,
      int maxHops) {
    return delegate.getLineage(opContext, entityUrn, lineageGraphFilters, offset, count, maxHops);
  }

  @Override
  public LineageResponse getImpactLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters filters,
      int maxHops) {
    return delegate.getImpactLineage(opContext, entityUrn, filters, maxHops);
  }

  @Override
  public SearchResponse getSearchResponse(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters filters,
      int offset,
      @Nullable Integer count) {
    return delegate.getSearchResponse(opContext, filters, offset, count);
  }

  @Override
  public SearchResponse getSearchResponse(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters filters,
      @Nonnull List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer count) {
    return delegate.getSearchResponse(opContext, filters, sortCriteria, scrollId, keepAlive, count);
  }

  SearchResponse executeSearch(@Nonnull SearchRequest searchRequest) {
    return delegate.executeSearch(searchRequest);
  }
}
