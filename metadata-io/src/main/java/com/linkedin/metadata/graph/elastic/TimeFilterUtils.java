package com.linkedin.metadata.graph.elastic;

import static com.linkedin.metadata.graph.elastic.ESGraphQueryDAO.*;

import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

@Slf4j
public class TimeFilterUtils {

  /**
   * In order to filter for edges that fall into a specific filter window, we perform a
   * range-overlap query. Note that both a start time and an end time must be provided in order to
   * add the filters.
   *
   * <p>A range overlap query compares 2 time windows for ANY overlap. This essentially equates to a
   * union operation. Each window is characterized by 2 points in time: a start time (e.g. created
   * time of the edge) and an end time (e.g. last updated time of an edge).
   *
   * @param startTimeMillis the start of the time filter window
   * @param endTimeMillis the end of the time filter window
   */
  public static QueryBuilder getEdgeTimeFilterQuery(
      final long startTimeMillis, final long endTimeMillis) {
    log.debug(
        String.format(
            "Adding edge time filters for start time: %s, end time: %s",
            startTimeMillis, endTimeMillis));
    /*
     * One of the following must be true in order for the edge to be returned (should = OR)
     *
     * 1. The start and end time window should overlap with the createdOn updatedOn window.
     * 2. The createdOn and updatedOn window does not exist on the edge at all (support legacy cases)
     * 3. Special lineage case: The edge is marked as a "manual" edge, meaning that the time filters should NOT be applied.
     */
    BoolQueryBuilder timeFilterQuery = QueryBuilders.boolQuery();
    timeFilterQuery.should(buildTimeWindowFilter(startTimeMillis, endTimeMillis));
    timeFilterQuery.should(buildTimestampsMissingFilter());
    timeFilterQuery.should(buildManualLineageFilter());
    return timeFilterQuery;
  }

  /**
   * Builds a filter that compares 2 windows on a timeline and returns true for any overlap. This
   * logic is a bit tricky so change with caution.
   *
   * <p>The first window comes from start time and end time provided by the user. The second window
   * comes from the createdOn and updatedOn timestamps present on graph edges.
   *
   * <p>Also accounts for the case where createdOn or updatedOn is MISSING, and in such cases
   * performs a point overlap instead of a range overlap.
   *
   * <p>Range Examples:
   *
   * <p>start time -> end time |-----| createdOn -> updatedOn |-----|
   *
   * <p>= true
   *
   * <p>start time -> end time |------| createdOn -> updatedOn |--|
   *
   * <p>= true
   *
   * <p>start time -> end time |-----| createdOn -> updatedOn |-----|
   *
   * <p>= true
   *
   * <p>start time -> end time |-----| createdOn -> updatedOn |-----|
   *
   * <p>= false
   *
   * <p>Point Examples:
   *
   * <p>start time -> end time |-----| updatedOn |
   *
   * <p>= true
   *
   * <p>start time -> end time |-----| updatedOn |
   *
   * <p>= false
   *
   * <p>and same for createdOn.
   *
   * <p>Assumptions are that startTimeMillis is always before or equal to endTimeMillis, and
   * createdOn is always before or equal to updatedOn.
   *
   * @param startTimeMillis the start time of the window in milliseconds
   * @param endTimeMillis the end time of the window in milliseconds
   * @return Query Builder with time window filters appended.
   */
  private static QueryBuilder buildTimeWindowFilter(
      final long startTimeMillis, final long endTimeMillis) {
    final BoolQueryBuilder timeWindowQuery = QueryBuilders.boolQuery();

    /*
     * To perform comparison:
     *
     * If either createdOn or updatedOn time point falls into the startTime->endTime window,
     * the edge should be included.
     *
     * We also verify that the field actually exists (non-null).
     */

    // Build filter comparing createdOn time to startTime->endTime window.
    BoolQueryBuilder createdOnFilter = QueryBuilders.boolQuery();
    createdOnFilter.must(QueryBuilders.existsQuery(CREATED_ON));
    createdOnFilter.must(
        QueryBuilders.rangeQuery(CREATED_ON).gte(startTimeMillis).lte(endTimeMillis));

    // Build filter comparing updatedOn time to startTime->endTime window.
    BoolQueryBuilder updatedOnFilter = QueryBuilders.boolQuery();
    updatedOnFilter.must(QueryBuilders.existsQuery(UPDATED_ON));
    updatedOnFilter.must(
        QueryBuilders.rangeQuery(UPDATED_ON).gte(startTimeMillis).lte(endTimeMillis));

    // Now - OR the 2 point comparison conditions together.
    timeWindowQuery.should(createdOnFilter);
    timeWindowQuery.should(updatedOnFilter);
    return timeWindowQuery;
  }

  private static QueryBuilder buildTimestampsMissingFilter() {
    // If both createdOn and updatedOn do NOT EXIST (either are null or 0), then
    // return the edge.
    final BoolQueryBuilder boolExistenceBuilder = QueryBuilders.boolQuery();
    boolExistenceBuilder.must(buildNotExistsFilter(CREATED_ON));
    boolExistenceBuilder.must(buildNotExistsFilter(UPDATED_ON));
    return boolExistenceBuilder;
  }

  private static QueryBuilder buildNotExistsFilter(String fieldName) {
    // This filter returns 'true' if the field DOES NOT EXIST or it exists but is equal to 0.
    final BoolQueryBuilder notExistsFilter = QueryBuilders.boolQuery();
    notExistsFilter.should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(fieldName)));
    notExistsFilter.should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery(fieldName, 0L)));
    return notExistsFilter;
  }

  private static QueryBuilder buildManualLineageFilter() {
    return QueryBuilders.termQuery(String.format("%s.%s", PROPERTIES, SOURCE), UI);
  }

  private TimeFilterUtils() {}
}
