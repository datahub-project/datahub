package com.linkedin.metadata.timeseries.postgres;

import static com.linkedin.metadata.utils.CriterionUtils.buildConjunctiveCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.TimeWindowSize;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class TimeseriesFilterSqlBuilderTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @Test
  public void stripKeywordSuffix_removesTrailingKeyword() {
    assertEquals(TimeseriesPgJsonPaths.stripKeywordSuffix("browsePaths.keyword"), "browsePaths");
    assertEquals(TimeseriesPgJsonPaths.stripKeywordSuffix("urn"), "urn");
  }

  @Test
  public void documentTextPathSql_singleSegment() {
    String sql = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql("timestampMillis");
    assertTrue(sql.contains("document->>'timestampMillis'"));
  }

  @Test
  public void documentTextPathSql_nested() {
    String sql = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql("userCounts.usageCount");
    assertTrue(sql.contains("document #>> "));
    assertTrue(sql.contains("'userCounts'"));
    assertTrue(sql.contains("'usageCount'"));
  }

  @Test
  public void postgresDateTrunc_mapsAllCalendarIntervalUnits() {
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.postgresDateTrunc(
            new TimeWindowSize().setUnit(CalendarInterval.SECOND).setMultiple(1)),
        "second");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.postgresDateTrunc(
            new TimeWindowSize().setUnit(CalendarInterval.MINUTE).setMultiple(1)),
        "minute");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.postgresDateTrunc(
            new TimeWindowSize().setUnit(CalendarInterval.HOUR).setMultiple(1)),
        "hour");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.postgresDateTrunc(
            new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(1)),
        "day");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.postgresDateTrunc(
            new TimeWindowSize().setUnit(CalendarInterval.WEEK).setMultiple(1)),
        "week");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.postgresDateTrunc(
            new TimeWindowSize().setUnit(CalendarInterval.MONTH).setMultiple(1)),
        "month");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.postgresDateTrunc(
            new TimeWindowSize().setUnit(CalendarInterval.QUARTER).setMultiple(1)),
        "quarter");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.postgresDateTrunc(
            new TimeWindowSize().setUnit(CalendarInterval.YEAR).setMultiple(1)),
        "year");
  }

  @Test
  public void buildDocumentFilter_nullFilter_isTrue() {
    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            null, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);
    assertEquals(built.getExpression(), "TRUE");
    assertTrue(built.getParams().isEmpty());
  }

  @Test
  public void buildDocumentFilter_timestampMillisRange_usesEventTimeColumn() {
    Filter filter =
        QueryUtils.getFilterFromCriteria(
            List.of(
                buildCriterion(
                    "timestampMillis", Condition.LESS_THAN_OR_EQUAL_TO, "1700000000000")));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains("event_time <="));
    assertTrue(!built.getExpression().contains("timestampMillis"));
    assertEquals(built.getParams().size(), 1);
    assertTrue(built.getParams().get(0) instanceof java.time.OffsetDateTime);
    assertEquals(
        ((java.time.OffsetDateTime) built.getParams().get(0)).toInstant().toEpochMilli(),
        1700000000000L);
  }

  @Test
  public void buildDocumentFilter_equal_isCaseSensitive() {
    Filter filter =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("strStat", Condition.EQUAL, "Foo")));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains("document->>'strStat' = ?"));
    assertTrue(!built.getExpression().contains("LOWER("));
    assertEquals(built.getParams(), List.of("Foo"));
  }

  @Test
  public void buildDocumentFilter_iequal_usesLowerCompare() {
    Filter filter =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("strStat", Condition.IEQUAL, "Foo")));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains("LOWER("));
    assertTrue(built.getExpression().contains("= LOWER(?)"));
    assertEquals(built.getParams(), List.of("Foo"));
  }

  @Test
  public void buildDocumentFilter_keywordSuffix_strippedFromField() {
    Filter filter =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("urn.keyword", Condition.EQUAL, "urn:li:dataset:x")));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains("document->>'urn' = ?"));
    assertTrue(!built.getExpression().contains("urn.keyword"));
  }

  @Test
  public void buildDocumentFilter_nestedField_usesHashPath() {
    Filter filter =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("componentProfiles.key", Condition.EQUAL, "col1")));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains("document #>> ARRAY[?,?]::text[]"));
    assertEquals(built.getParams(), List.of("componentProfiles", "key", "col1"));
  }

  @Test
  public void buildDocumentFilter_exists_and_isNull() {
    Filter existsFilter =
        QueryUtils.getFilterFromCriteria(List.of(buildExistsCriterion("strStat")));
    TimeseriesFilterSqlBuilder.BuiltSql exists =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            existsFilter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);
    assertTrue(exists.getExpression().contains("IS NOT NULL"));
    assertTrue(exists.getExpression().contains("<> ''"));

    Filter nullFilter = QueryUtils.getFilterFromCriteria(List.of(buildIsNullCriterion("strStat")));
    TimeseriesFilterSqlBuilder.BuiltSql isNull =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            nullFilter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);
    assertTrue(isNull.getExpression().contains("IS NULL OR"));
  }

  @Test
  public void buildDocumentFilter_contain_startWith_endWith() {
    Filter contain =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("strStat", Condition.CONTAIN, "mid")));
    TimeseriesFilterSqlBuilder.BuiltSql containSql =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            contain, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);
    assertTrue(containSql.getExpression().contains("LIKE ? ESCAPE '\\'"));
    assertEquals(containSql.getParams(), List.of("%mid%"));

    Filter start =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("strStat", Condition.START_WITH, "pre")));
    TimeseriesFilterSqlBuilder.BuiltSql startSql =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            start, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);
    assertEquals(startSql.getParams(), List.of("pre%"));

    Filter end =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("strStat", Condition.END_WITH, "suf")));
    TimeseriesFilterSqlBuilder.BuiltSql endSql =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            end, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);
    assertEquals(endSql.getParams(), List.of("%suf"));
  }

  @Test
  public void buildDocumentFilter_range_greaterThan() {
    Filter filter =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("timestampMillis", Condition.GREATER_THAN, "100")));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains("event_time > ?"));
    assertEquals(built.getParams().size(), 1);
    assertTrue(built.getParams().get(0) instanceof java.time.OffsetDateTime);
    assertEquals(
        ((java.time.OffsetDateTime) built.getParams().get(0)).toInstant().toEpochMilli(), 100L);
  }

  @Test
  public void buildDocumentFilter_disjunctiveOr_joinsWithOr() {
    Filter filter =
        QueryUtils.newDisjunctiveFilter(
            buildCriterion("strStat", Condition.EQUAL, "a"),
            buildCriterion("strStat", Condition.EQUAL, "b"));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains(" OR "));
    assertEquals(built.getParams(), List.of("a", "b"));
  }

  @Test
  public void buildDocumentFilter_conjunctiveAnd_joinsWithAnd() {
    Filter filter =
        QueryUtils.newDisjunctiveFilter(
            buildConjunctiveCriterion(
                buildCriterion("strStat", Condition.EQUAL, "a"),
                buildCriterion("urn", Condition.EQUAL, "urn:li:dataset:x")));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains(" AND "));
    assertEquals(built.getParams(), List.of("a", "urn:li:dataset:x"));
  }

  @Test
  public void buildDocumentFilter_negatedEqual_wrapsNot() {
    Filter filter =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("strStat", Condition.EQUAL, true, "Foo")));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), opContext, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains("NOT ("));
    assertEquals(built.getParams(), List.of("Foo"));
  }

  @Test
  public void buildDocumentFilter_filterNonLatestVersions_appendsIsLatestClause() {
    OperationContext withFlag =
        opContext.withSearchFlags(flags -> flags.setFilterNonLatestVersions(true));
    Filter filter =
        QueryUtils.getFilterFromCriteria(
            List.of(buildCriterion("urn", Condition.EQUAL, "urn:li:dataset:x")));

    TimeseriesFilterSqlBuilder.BuiltSql built =
        TimeseriesFilterSqlBuilder.buildDocumentFilter(
            filter, true, Collections.emptyMap(), withFlag, QueryFilterRewriteChain.EMPTY);

    assertTrue(built.getExpression().contains("document->>'isLatest' = 'true'"));
    assertTrue(built.getExpression().contains("document->>'isLatest' IS NULL"));
  }
}
