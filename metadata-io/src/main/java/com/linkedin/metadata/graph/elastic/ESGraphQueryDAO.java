package com.linkedin.metadata.graph.elastic;

import com.codahale.metrics.Timer;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.INDEX_NAME;


/**
 * A search DAO for Elasticsearch backend.
 */
@Slf4j
@RequiredArgsConstructor
public class ESGraphQueryDAO {

  private final RestHighLevelClient client;
  private final IndexConvention indexConvention;

  @Nonnull
  public static void addFilterToQueryBuilder(@Nonnull Filter filter, String node, BoolQueryBuilder rootQuery) {
    BoolQueryBuilder orQuery = new BoolQueryBuilder();
    for (ConjunctiveCriterion conjunction : filter.getOr()) {
      final BoolQueryBuilder andQuery = new BoolQueryBuilder();
      final List<Criterion> criterionArray = conjunction.getAnd();
      if (!criterionArray.stream().allMatch(criterion -> Condition.EQUAL.equals(criterion.getCondition()))) {
        throw new RuntimeException("Currently Elastic query filter only supports EQUAL condition " + criterionArray);
      }
      criterionArray.forEach(
          criterion -> andQuery.must(
              QueryBuilders.termQuery(node + "." + criterion.getField(), criterion.getValue())
          )
      );
      orQuery.should(andQuery);
    }
    rootQuery.must(orQuery);
  }

  public SearchResponse getSearchResponse(
      @Nullable final String sourceType,
      @Nonnull  final Filter sourceEntityFilter,
      @Nullable final String destinationType,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter,
      final int offset,
      final int count) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.from(offset);
    searchSourceBuilder.size(count);

    BoolQueryBuilder finalQuery = buildQuery(
        sourceType,
        sourceEntityFilter,
        destinationType,
        destinationEntityFilter,
        relationshipTypes,
        relationshipFilter
    );

    searchSourceBuilder.query(finalQuery);

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "esQuery").time()) {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      return searchResponse;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static BoolQueryBuilder buildQuery(
      @Nullable final String sourceType,
      @Nonnull  final Filter sourceEntityFilter,
      @Nullable final String destinationType,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter
  ) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    final RelationshipDirection relationshipDirection = relationshipFilter.getDirection();

    // set source filter
    String sourceNode = relationshipDirection == RelationshipDirection.OUTGOING ? "source" : "destination";
    if (sourceType != null && sourceType.length() > 0) {
      finalQuery.must(QueryBuilders.termQuery(sourceNode + ".entityType", sourceType));
    }
    addFilterToQueryBuilder(sourceEntityFilter, sourceNode, finalQuery);

    // set destination filter
    String destinationNode = relationshipDirection == RelationshipDirection.OUTGOING ? "destination" : "source";
    if (destinationType != null && destinationType.length() > 0) {
      finalQuery.must(QueryBuilders.termQuery(destinationNode + ".entityType", destinationType));
    }
    addFilterToQueryBuilder(destinationEntityFilter, destinationNode, finalQuery);

    // set relationship filter
    if (relationshipTypes.size() > 0) {
      BoolQueryBuilder relationshipQuery = QueryBuilders.boolQuery();
      relationshipTypes.forEach(relationshipType
          -> relationshipQuery.should(QueryBuilders.termQuery("relationshipType", relationshipType)));
      finalQuery.must(relationshipQuery);
    }
    return finalQuery;
  }

}
