package com.linkedin.metadata.timeseries.postgres;

import static io.datahubproject.test.search.SearchTestUtils.TEST_TIMESERIES_ASPECT_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.test.BatchType;
import com.datahub.test.ComplexNestedRecord;
import com.datahub.test.TestEntityComponentProfile;
import com.datahub.test.TestEntityComponentProfileArray;
import com.datahub.test.TestEntityProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.template.StringMapArray;
import com.linkedin.metadata.EbeanTestUtils;
import com.linkedin.metadata.PostgresTestUtils;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.MappingsBuilder;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.metadata.timeseries.write.postgres.PostgresTimeseriesAspectWriteSink;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import com.linkedin.timeseries.TimeseriesIndexSizeResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.search.SearchTestUtils;
import io.ebean.Database;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PostgreSQL/Testcontainers counterpart to {@link
 * com.linkedin.metadata.search.elasticsearch.TimeseriesAspectServiceElasticSearchTest} / {@link
 * com.linkedin.metadata.timeseries.search.TimeseriesAspectServiceTestBase}: exercises {@link
 * PostgresTimeseriesAspectService} + {@link PostgresTimeseriesAspectWriteSink} against a real DB.
 */
public class TimeseriesAspectServicePostgresIT {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private TimeZone defaultTimeZoneBackup;

  private static final String ENTITY_NAME = "testEntity";
  private static final String ASPECT_NAME = "testEntityProfile";
  private static final Urn TEST_URN =
      new TestEntityUrn("acryl", "testPostgresTimeseriesAspectService", "table1");
  private static final int NUM_PROFILES = 100;
  private static final long TIME_INCREMENT = 3600000L;
  private static final String CONTENT_TYPE = "application/json";

  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  private static final String TS_PROFILE_RESOURCE = "test-entity-registry.yml";

  private String schema;
  private String tablePrefix;

  private Database database;
  private PostgresSqlSetupProperties props;
  private PostgresTimeseriesAspectWriteSink writeSink;
  private PostgresTimeseriesAspectService pgTimeseries;
  private OperationContext opContext;
  private AspectSpec aspectSpec;

  private Map<Long, TestEntityProfile> testEntityProfiles;
  private Long startTime;

  @BeforeClass
  public void beforeClass() throws Exception {
    defaultTimeZoneBackup = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

    PostgresTestUtils.IntegrationNamespace ns =
        PostgresTestUtils.newIntegrationNamespace("timeseries");
    schema = ns.getSchema();
    tablePrefix = ns.getTablePrefix();

    PostgreSQLContainer<?> postgres = PostgresTestUtils.startPostgres();
    props = PostgresTestUtils.testPgTimeseriesProperties(schema, tablePrefix);
    database =
        PostgresTestUtils.createEbeanDatabase(
            postgres, PostgresTestUtils.uniqueServerName("pg_ts_aspect_it"));

    try (java.sql.Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.applyPgTimeseriesAspectRowTable(c, props);
    }

    EntityRegistry entityRegistry =
        new ConfigEntityRegistry(
            new DataSchemaFactory("com.datahub.test"),
            List.of(),
            TestEntityProfile.class.getClassLoader().getResourceAsStream(TS_PROFILE_RESOURCE));

    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("pg_timeseries_aspect_service_it")
                .hashIdAlgo("MD5")
                .build(),
            SearchTestUtils.DEFAULT_ENTITY_INDEX_CONFIGURATION);

    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            entityRegistry,
            SearchContext.EMPTY.toBuilder().indexConvention(indexConvention).build());

    EntitySpec entitySpec = entityRegistry.getEntitySpec(ENTITY_NAME);
    aspectSpec = entitySpec.getAspectSpec(ASPECT_NAME);

    writeSink =
        new PostgresTimeseriesAspectWriteSink(new PostgresTimeseriesAspectDao(database, props));
    pgTimeseries =
        new PostgresTimeseriesAspectService(
            database,
            props,
            TEST_TIMESERIES_ASPECT_SERVICE_CONFIG,
            QueryFilterRewriteChain.EMPTY,
            entityRegistry);
  }

  @AfterClass(alwaysRun = true)
  public void afterClass() {
    if (defaultTimeZoneBackup != null) {
      TimeZone.setDefault(defaultTimeZoneBackup);
    }
    EbeanTestUtils.shutdownDatabase(database);
  }

  @BeforeMethod
  public void resetAndSeed() throws Exception {
    try (java.sql.Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.truncatePgTimeseriesAspectRow(c, props);
    }
    seedStandardProfiles();
  }

  private void seedStandardProfiles() throws JsonProcessingException {
    startTime = Calendar.getInstance().getTimeInMillis();
    startTime = startTime - startTime % 86400000;

    TestEntityProfile firstProfile = makeTestProfile(startTime, 20, null);
    Stream<TestEntityProfile> stream =
        Stream.iterate(
            firstProfile,
            prev ->
                makeTestProfile(
                    prev.getTimestampMillis() + TIME_INCREMENT, prev.getStat() + 10, null));

    testEntityProfiles =
        stream
            .limit(NUM_PROFILES)
            .collect(Collectors.toMap(TestEntityProfile::getTimestampMillis, Function.identity()));

    for (TestEntityProfile p : testEntityProfiles.values()) {
      upsertAllTransformedDocs(p, TEST_URN);
    }
  }

  private void upsertAllTransformedDocs(TestEntityProfile profile, Urn urn)
      throws JsonProcessingException {
    Map<String, JsonNode> documents =
        TimeseriesAspectTransformer.transform(urn, profile, aspectSpec, null, "MD5");
    assertEquals(documents.size(), 3);
    documents.forEach(
        (key, value) -> writeSink.upsertDocument(opContext, ENTITY_NAME, ASPECT_NAME, key, value));
  }

  private TestEntityProfile makeTestProfile(long eventTime, long stat, String messageId) {
    TestEntityProfile testEntityProfile = new TestEntityProfile();
    testEntityProfile.setTimestampMillis(eventTime);
    testEntityProfile.setStat(stat);
    testEntityProfile.setStrStat(String.valueOf(stat));
    testEntityProfile.setStrArray(new StringArray("sa_" + stat, "sa_" + (stat + 1)));
    testEntityProfile.setEventGranularity(
        new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(1));
    if (messageId != null) {
      testEntityProfile.setMessageId(messageId);
    }

    TestEntityComponentProfile componentProfile1 = new TestEntityComponentProfile();
    componentProfile1.setKey("col1");
    componentProfile1.setStat(stat + 1);
    TestEntityComponentProfile componentProfile2 = new TestEntityComponentProfile();
    componentProfile2.setKey("col2");
    componentProfile2.setStat(stat + 2);
    testEntityProfile.setComponentProfiles(
        new TestEntityComponentProfileArray(componentProfile1, componentProfile2));

    StringMap stringMap1 = new StringMap();
    stringMap1.put("p_key1", "p_val1");
    StringMap stringMap2 = new StringMap();
    stringMap2.put("p_key2", "p_val2");
    ComplexNestedRecord nestedRecord =
        new ComplexNestedRecord()
            .setType(BatchType.PARTITION_BATCH)
            .setPartitions(new StringMapArray(stringMap1, stringMap2));
    testEntityProfile.setAComplexNestedRecord(nestedRecord);

    return testEntityProfile;
  }

  private void validateAspectValue(EnvelopedAspect envelopedAspectResult) {
    TestEntityProfile actualProfile =
        (TestEntityProfile)
            GenericRecordUtils.deserializeAspect(
                envelopedAspectResult.getAspect().getValue(), CONTENT_TYPE, aspectSpec);
    TestEntityProfile expectedProfile = testEntityProfiles.get(actualProfile.getTimestampMillis());
    assertNotNull(expectedProfile);
    assertEquals(actualProfile.getStat(), expectedProfile.getStat());
    assertEquals(actualProfile.getTimestampMillis(), expectedProfile.getTimestampMillis());
  }

  private void validateAspectValues(List<EnvelopedAspect> aspects, long numResultsExpected) {
    assertEquals(aspects.size(), numResultsExpected);
    aspects.forEach(this::validateAspectValue);
  }

  /**
   * Elasticsearch {@code getAspectValues} returns only documents with a full top-level {@code
   * event}; collection-exploded rows omit it. Mirror that with {@code event} EXISTS on {@code
   * document}.
   */
  private static Filter withPrimaryAspectDocumentFilter(@Nullable Filter base) {
    Criterion hasAspectLevelEvent =
        com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion(
            MappingsBuilder.EVENT_FIELD);
    if (base == null || base.getCriteria() == null || base.getCriteria().isEmpty()) {
      return com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
          Collections.singletonList(hasAspectLevelEvent));
    }
    List<Criterion> criteria = new ArrayList<>();
    for (Criterion c : base.getCriteria()) {
      criteria.add(c);
    }
    criteria.add(hasAspectLevelEvent);
    return com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(criteria);
  }

  @Test
  public void getAspectValues_defaultOrder_descendingTime_highestStatFirst() {
    List<EnvelopedAspect> resultAspects =
        pgTimeseries.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            null,
            null,
            NUM_PROFILES,
            withPrimaryAspectDocumentFilter(null));
    validateAspectValues(resultAspects, NUM_PROFILES);

    TestEntityProfile firstProfile =
        (TestEntityProfile)
            GenericRecordUtils.deserializeAspect(
                resultAspects.get(0).getAspect().getValue(), CONTENT_TYPE, aspectSpec);
    TestEntityProfile lastProfile =
        (TestEntityProfile)
            GenericRecordUtils.deserializeAspect(
                resultAspects.get(resultAspects.size() - 1).getAspect().getValue(),
                CONTENT_TYPE,
                aspectSpec);

    assertEquals((long) firstProfile.getStat(), 20 + (NUM_PROFILES - 1) * 10);
    assertEquals((long) lastProfile.getStat(), 20);
  }

  /**
   * Uses {@link MappingsBuilder#TIMESTAMP_MILLIS_FIELD} for sorting — text ordering on {@code stat}
   * in JSON would not match numeric ES order.
   */
  @Test
  public void getAspectValues_sortedByTimestampMillis_ascending_matchesStatOrder() {
    List<EnvelopedAspect> resultAspects =
        pgTimeseries.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            null,
            null,
            NUM_PROFILES,
            withPrimaryAspectDocumentFilter(null),
            new SortCriterion()
                .setField(MappingsBuilder.TIMESTAMP_MILLIS_FIELD)
                .setOrder(SortOrder.ASCENDING));
    validateAspectValues(resultAspects, NUM_PROFILES);

    TestEntityProfile firstProfile =
        (TestEntityProfile)
            GenericRecordUtils.deserializeAspect(
                resultAspects.get(0).getAspect().getValue(), CONTENT_TYPE, aspectSpec);
    TestEntityProfile lastProfile =
        (TestEntityProfile)
            GenericRecordUtils.deserializeAspect(
                resultAspects.get(resultAspects.size() - 1).getAspect().getValue(),
                CONTENT_TYPE,
                aspectSpec);

    assertEquals((long) firstProfile.getStat(), 20);
    assertEquals((long) lastProfile.getStat(), 20 + (NUM_PROFILES - 1) * 10);
  }

  @Test
  public void getAspectValues_filterStatEqualsOneRow() {
    Filter filter = new Filter();
    filter.setCriteria(
        new CriterionArray(
            com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                "stat", Condition.EQUAL, "20")));
    List<EnvelopedAspect> resultAspects =
        pgTimeseries.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            null,
            null,
            NUM_PROFILES,
            withPrimaryAspectDocumentFilter(filter));
    validateAspectValues(resultAspects, 1);
  }

  @Test
  public void getAspectValues_timeRangeInclusiveOverlap() {
    int expectedNumRows = 10;
    List<EnvelopedAspect> resultAspects =
        pgTimeseries.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            startTime,
            startTime + TIME_INCREMENT * (expectedNumRows - 1),
            expectedNumRows,
            withPrimaryAspectDocumentFilter(null));
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test
  public void getAspectValues_missingUrn_returnsEmpty() {
    Urn nonExistingUrn = new TestEntityUrn("missing", "missing", "missing");
    List<EnvelopedAspect> resultAspects =
        pgTimeseries.getAspectValues(
            opContext, nonExistingUrn, ENTITY_NAME, ASPECT_NAME, null, null, NUM_PROFILES, null);
    assertEquals(resultAspects.size(), 0);
  }

  /**
   * Mirrors {@link
   * com.linkedin.metadata.timeseries.search.TimeseriesAspectServiceTestBase#testUpsertProfilesWithUniqueMessageIds}.
   *
   * <p>Postgres stores one row per {@code (entity, aspect, message_id, event_time)}; collection
   * exploded documents share the aspect {@code messageId}, so the row count is the number of
   * distinct message ids (here 3), not 3 transformer outputs × 3 docs (9) as in Elasticsearch.
   */
  @Test
  public void upsert_sameTimestampDistinctMessageIds_returnsAllRows() throws Exception {
    try (java.sql.Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.truncatePgTimeseriesAspectRow(c, props);
    }

    final long curTimeMillis = Calendar.getInstance().getTimeInMillis();
    final long dayStart = curTimeMillis - curTimeMillis % 86400000;
    final TestEntityProfile firstProfile = makeTestProfile(dayStart, 20, "20");
    Stream<TestEntityProfile> stream =
        Stream.iterate(
            firstProfile,
            prev ->
                makeTestProfile(
                    prev.getTimestampMillis(),
                    prev.getStat() + 10,
                    String.valueOf(prev.getStat() + 10)));

    List<TestEntityProfile> profiles = stream.limit(3).collect(Collectors.toList());
    Urn urn = new TestEntityUrn("acryl", "testPostgresTimeseriesAspectService", "table2");
    for (TestEntityProfile p : profiles) {
      upsertAllTransformedDocs(p, urn);
    }

    Filter urnFilter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            Collections.singletonList(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, urn.toString())));
    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnFilter), 3L);

    List<EnvelopedAspect> resultAspects =
        pgTimeseries.getAspectValues(
            opContext, urn, ENTITY_NAME, ASPECT_NAME, null, null, profiles.size(), null);
    assertEquals(resultAspects.size(), profiles.size());
  }

  @Test
  public void countByFilter_matchesElasticsearchIntegrationExpectations() {
    Filter urnFilter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            Collections.singletonList(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString())));

    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnFilter), 300L);

    Filter urnAndTime =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            List.of(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP,
                    Condition.LESS_THAN_OR_EQUAL_TO,
                    String.valueOf(startTime + 23 * TIME_INCREMENT))));

    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnAndTime), 72L);

    assertEquals(
        pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, new Filter()), 300L);
  }

  @Test
  public void aggregatedStats_sum_firstTenHoursOfDay_oneRowSum650() {
    Filter filter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            List.of(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP,
                    Condition.LESS_THAN_OR_EQUAL_TO,
                    String.valueOf(startTime + 9 * TIME_INCREMENT))));

    AggregationSpec sumSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("stat");

    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable table =
        pgTimeseries.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {sumSpec},
            filter,
            new GroupingBucket[] {timestampBucket});

    assertEquals(
        table.getRows(), new StringArrayArray(new StringArray(startTime.toString(), "650.0")));
  }

  @Test
  public void aggregatedStats_latest_stat_day_one_matchesLastHourOfDay() {
    Filter filter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            List.of(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP,
                    Condition.LESS_THAN_OR_EQUAL_TO,
                    String.valueOf(startTime + 23 * TIME_INCREMENT))));

    AggregationSpec latestStat =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("stat");

    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable table =
        pgTimeseries.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {latestStat},
            filter,
            new GroupingBucket[] {timestampBucket});

    assertEquals(
        table.getRows(),
        new StringArrayArray(
            new StringArray(
                startTime.toString(),
                testEntityProfiles.get(startTime + 23 * TIME_INCREMENT).getStat().toString())));
  }

  @Test
  public void aggregatedStats_cardinality_strStat_firstDay() {
    Filter filter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            List.of(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP,
                    Condition.LESS_THAN_OR_EQUAL_TO,
                    String.valueOf(startTime + 23 * TIME_INCREMENT))));

    AggregationSpec cardinalitySpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("strStat");

    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable table =
        pgTimeseries.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {cardinalitySpec},
            filter,
            new GroupingBucket[] {timestampBucket});

    assertEquals(
        table.getRows(), new StringArrayArray(new StringArray(startTime.toString(), "24")));
  }

  @Test
  public void deleteAspectValues_thenCount_matchesElasticsearchDocCounts() {
    Filter day1 =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            List.of(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP,
                    Condition.LESS_THAN_OR_EQUAL_TO,
                    String.valueOf(startTime + 23 * TIME_INCREMENT))));

    DeleteAspectValuesResult del1 =
        pgTimeseries.deleteAspectValues(opContext, ENTITY_NAME, ASPECT_NAME, day1);
    assertEquals(del1.getNumDocsDeleted(), Long.valueOf(72L));

    Filter urnOnly =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            Collections.singletonList(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString())));
    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnOnly), 228L);
    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, day1), 0L);

    DeleteAspectValuesResult del2 =
        pgTimeseries.deleteAspectValues(opContext, ENTITY_NAME, ASPECT_NAME, urnOnly);
    assertEquals(del2.getNumDocsDeleted(), Long.valueOf(228L));
  }

  @Test
  public void getIndexSizes_returnsQualifiedTable() {
    List<TimeseriesIndexSizeResult> sizes = pgTimeseries.getIndexSizes(opContext);
    assertTrue(sizes.size() > 0);
    String qualified = schema + "." + tablePrefix + "_aspect_row";
    assertTrue(sizes.stream().anyMatch(r -> qualified.equals(r.getIndexName())));
  }
}
