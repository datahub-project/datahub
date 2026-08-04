package com.linkedin.metadata.timeseries.postgres;

import static io.datahubproject.test.search.SearchTestUtils.TEST_TIMESERIES_ASPECT_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
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
import com.linkedin.metadata.timeseries.GenericTimeseriesDocument;
import com.linkedin.metadata.timeseries.TimeseriesScrollResult;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
      PostgresTestUtils.applyPgTimeseriesAspectTable(c, props);
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
      PostgresTestUtils.truncatePgTimeseriesAspect(c, props);
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
   * Minute {@code date_trunc} must produce distinct buckets for events in the same hour (not
   * collapse to a single hour bucket).
   */
  @Test
  public void aggregatedStats_minuteBuckets_sameHour_twoDistinctBuckets() throws Exception {
    try (java.sql.Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.truncatePgTimeseriesAspect(c, props);
    }

    Urn urn = new TestEntityUrn("acryl", "testPostgresTimeseriesAspectService", "tableMinuteAgg");
    long hourStart = Calendar.getInstance().getTimeInMillis();
    hourStart = hourStart - hourStart % 3_600_000L;
    long minute0 = hourStart;
    long minute1 = hourStart + 60_000L;

    TestEntityProfile profile0 = makeTestProfile(minute0, 10, "minute-bucket-0");
    TestEntityProfile profile1 = makeTestProfile(minute1, 20, "minute-bucket-1");
    for (TestEntityProfile profile : List.of(profile0, profile1)) {
      Map<String, JsonNode> documents =
          TimeseriesAspectTransformer.transform(urn, profile, aspectSpec, null, "MD5");
      // Upsert the non-exploded document so SUM(stat) is not overwritten by collection explosions.
      for (Map.Entry<String, JsonNode> entry : documents.entrySet()) {
        JsonNode doc = entry.getValue();
        boolean exploded =
            doc.has(MappingsBuilder.IS_EXPLODED_FIELD)
                && doc.get(MappingsBuilder.IS_EXPLODED_FIELD).asBoolean(false);
        if (!exploded) {
          pgTimeseries.upsertDocument(opContext, ENTITY_NAME, ASPECT_NAME, entry.getKey(), doc);
        }
      }
    }

    Filter urnFilter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            Collections.singletonList(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, urn.toString())));

    AggregationSpec sumSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("stat");
    GroupingBucket minuteBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(
                new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.MINUTE));

    GenericTable table =
        pgTimeseries.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {sumSpec},
            urnFilter,
            new GroupingBucket[] {minuteBucket});

    assertEquals(table.getRows().size(), 2);
    assertEquals(
        table.getRows(),
        new StringArrayArray(
            new StringArray(String.valueOf(minute0), "10.0"),
            new StringArray(String.valueOf(minute1), "20.0")));
  }

  /**
   * Upsert keys JDBC {@code message_id} from aspect {@code messageId} when set; delete must pass
   * the document so {@code resolveMessageId} matches (hashed ES {@code docId} alone misses the
   * row).
   */
  @Test
  public void deleteDocument_withAspectMessageId_removesUpsertedRow() throws Exception {
    try (java.sql.Connection c = database.dataSource().getConnection()) {
      c.setAutoCommit(false);
      PostgresTestUtils.truncatePgTimeseriesAspect(c, props);
    }

    Urn urn = new TestEntityUrn("acryl", "testPostgresTimeseriesAspectService", "tableMsgIdDel");
    long eventTime = Calendar.getInstance().getTimeInMillis();
    eventTime = eventTime - eventTime % 86400000;
    TestEntityProfile profile = makeTestProfile(eventTime, 42, "logical-msg-id");

    Map<String, JsonNode> documents =
        TimeseriesAspectTransformer.transform(urn, profile, aspectSpec, null, "MD5");
    assertEquals(documents.size(), 3);
    for (Map.Entry<String, JsonNode> entry : documents.entrySet()) {
      pgTimeseries.upsertDocument(
          opContext, ENTITY_NAME, ASPECT_NAME, entry.getKey(), entry.getValue());
    }

    Filter urnFilter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            Collections.singletonList(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, urn.toString())));
    // Exploded docs collapse onto the logical messageId.
    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnFilter), 1L);

    Map.Entry<String, JsonNode> first = documents.entrySet().iterator().next();
    pgTimeseries.deleteDocument(
        opContext, ENTITY_NAME, ASPECT_NAME, first.getKey(), first.getValue(), false);

    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnFilter), 0L);
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
      PostgresTestUtils.truncatePgTimeseriesAspect(c, props);
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
    String qualified = schema + "." + tablePrefix + "_aspect";
    assertTrue(sizes.stream().anyMatch(r -> qualified.equals(r.getIndexName())));
  }

  @Test
  public void deleteAspectValues_timestampCutoff_deletesOnlyOlderPrimaryRows() throws Exception {
    // Build one conjunctive filter (getFilterFromCriteria uses or/and, not top-level criteria).
    Filter urnFilter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            List.of(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion(
                    MappingsBuilder.EVENT_FIELD)));

    long before = pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnFilter);
    assertEquals(before, NUM_PROFILES);

    // Keep the newest 50 primary profiles; delete the older half by event_time.
    long cutoff = startTime + TIME_INCREMENT * 49;
    Filter cutoffFilter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            List.of(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString()),
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    ES_FIELD_TIMESTAMP, Condition.LESS_THAN_OR_EQUAL_TO, String.valueOf(cutoff)),
                com.linkedin.metadata.utils.CriterionUtils.buildExistsCriterion(
                    MappingsBuilder.EVENT_FIELD)));

    DeleteAspectValuesResult deleted =
        pgTimeseries.deleteAspectValues(opContext, ENTITY_NAME, ASPECT_NAME, cutoffFilter);
    assertEquals(deleted.getNumDocsDeleted(), Long.valueOf(50L));

    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnFilter), 50L);
  }

  @Test
  public void scrollAspects_timestampMessageIdDesc_pagesWithoutOverlapOrSkip() {
    List<SortCriterion> sort =
        List.of(
            new SortCriterion()
                .setField(MappingsBuilder.TIMESTAMP_MILLIS_FIELD)
                .setOrder(SortOrder.DESCENDING),
            new SortCriterion()
                .setField(MappingsBuilder.MESSAGE_ID_FIELD)
                .setOrder(SortOrder.DESCENDING));
    assertScrollPagesCoverAllPrimaryDocs(sort, /* pageSize= */ 10);
  }

  @Test
  public void scrollAspects_timestampAsc_pagesWithoutOverlapOrSkip() {
    List<SortCriterion> sort =
        List.of(
            new SortCriterion()
                .setField(MappingsBuilder.TIMESTAMP_MILLIS_FIELD)
                .setOrder(SortOrder.ASCENDING));
    List<Long> timestamps = assertScrollPagesCoverAllPrimaryDocs(sort, /* pageSize= */ 7);
    assertEquals(timestamps.get(0).longValue(), startTime.longValue());
    assertEquals(
        timestamps.get(timestamps.size() - 1).longValue(),
        startTime + TIME_INCREMENT * (NUM_PROFILES - 1));
  }

  @Test
  public void scrollAspects_customDocumentField_pagesWithoutOverlapOrSkip() {
    // strStat is text-ordered (same as getAspectValues); keyset must still not skip/dupe.
    List<SortCriterion> sort =
        List.of(
            new SortCriterion().setField("strStat").setOrder(SortOrder.ASCENDING),
            new SortCriterion()
                .setField(MappingsBuilder.MESSAGE_ID_FIELD)
                .setOrder(SortOrder.ASCENDING));
    assertScrollPagesCoverAllPrimaryDocs(sort, /* pageSize= */ 11);
  }

  @Test
  public void getLatestTimeseriesAspectValues_returnsNewestProfile() {
    Map<Urn, Map<String, EnvelopedAspect>> latest =
        pgTimeseries.getLatestTimeseriesAspectValues(
            opContext, Set.of(TEST_URN), Set.of(ASPECT_NAME), null);
    assertTrue(latest.containsKey(TEST_URN));
    EnvelopedAspect aspect = latest.get(TEST_URN).get(ASPECT_NAME);
    assertNotNull(aspect);
    assertNotNull(aspect.getAspect());
    assertTrue(aspect.getAspect().getValue().length() > 0);

    // Primary (non-exploded) latest profile via filtered getAspectValues for typed assertions.
    List<EnvelopedAspect> primary =
        pgTimeseries.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            null,
            null,
            1,
            withPrimaryAspectDocumentFilter(null));
    assertEquals(primary.size(), 1);
    TestEntityProfile profile =
        (TestEntityProfile)
            GenericRecordUtils.deserializeAspect(
                primary.get(0).getAspect().getValue(), CONTENT_TYPE, aspectSpec);
    assertEquals(
        profile.getTimestampMillis().longValue(), startTime + TIME_INCREMENT * (NUM_PROFILES - 1));
  }

  @Test
  public void raw_returnsLatestDocumentMap() {
    Map<Urn, Map<String, Map<String, Object>>> raw =
        pgTimeseries.raw(opContext, Map.of(TEST_URN.toString(), Set.of(ASPECT_NAME)));
    assertTrue(raw.containsKey(TEST_URN));
    Map<String, Object> doc = raw.get(TEST_URN).get(ASPECT_NAME);
    assertNotNull(doc);
    assertEquals(doc.get(MappingsBuilder.URN_FIELD), TEST_URN.toString());
  }

  @Test
  public void rollbackTimeseriesAspects_deletesByRunId() throws Exception {
    try (java.sql.Connection c = database.dataSource().getConnection();
        java.sql.Statement st = c.createStatement()) {
      int updated =
          st.executeUpdate(
              "UPDATE " + schema + "." + tablePrefix + "_aspect SET run_id = 'rollback-run'");
      assertTrue(updated > 0);
      c.commit();
    }

    DeleteAspectValuesResult rolled =
        pgTimeseries.rollbackTimeseriesAspects(opContext, "rollback-run");
    assertTrue(rolled.getNumDocsDeleted() > 0);

    Filter urnFilter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            List.of(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString())));
    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnFilter), 0L);
  }

  @Test
  public void deleteAspectValuesAsync_deletesMatchingRows() {
    Filter urnFilter =
        com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
            List.of(
                com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                    "urn", Condition.EQUAL, TEST_URN.toString())));
    long before = pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnFilter);
    assertTrue(before > 0);

    String taskId =
        pgTimeseries.deleteAspectValuesAsync(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            urnFilter,
            new com.linkedin.metadata.timeseries.BatchWriteOperationsOptions(50, 60));
    assertNotNull(taskId);
    assertEquals(pgTimeseries.countByFilter(opContext, ENTITY_NAME, ASPECT_NAME, urnFilter), 0L);
  }

  @Test
  public void supportsReindexForTruncate_isFalse_andReindexAsyncThrows() {
    assertTrue(!pgTimeseries.supportsReindexForTruncate());
    try {
      pgTimeseries.reindexAsync(
          opContext,
          ENTITY_NAME,
          ASPECT_NAME,
          new Filter(),
          new com.linkedin.metadata.timeseries.BatchWriteOperationsOptions());
      throw new AssertionError("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertTrue(expected.getMessage().contains("does not support reindex"));
    }
  }

  @Test
  public void aggregatedStats_stringGrouping_onStrStat() {
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
                    String.valueOf(startTime + 2 * TIME_INCREMENT))));

    AggregationSpec sumSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("stat");
    GroupingBucket stringBucket =
        new GroupingBucket().setKey("strStat").setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GenericTable table =
        pgTimeseries.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {sumSpec},
            filter,
            new GroupingBucket[] {stringBucket});

    assertTrue(table.getRows().size() >= 1);
    assertEquals(table.getColumnNames().get(0), "strStat");
  }

  /**
   * Pages through all primary (aspect-level) docs for {@link #TEST_URN} and asserts the union of
   * pages equals a single full-page scroll with no duplicates.
   *
   * @return timestamps in scroll order
   */
  private List<Long> assertScrollPagesCoverAllPrimaryDocs(
      List<SortCriterion> sortCriteria, int pageSize) {
    Filter filter =
        withPrimaryAspectDocumentFilter(
            com.linkedin.metadata.search.utils.QueryUtils.getFilterFromCriteria(
                Collections.singletonList(
                    com.linkedin.metadata.utils.CriterionUtils.buildCriterion(
                        "urn", Condition.EQUAL, TEST_URN.toString()))));

    TimeseriesScrollResult all =
        pgTimeseries.scrollAspects(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            filter,
            sortCriteria,
            null,
            NUM_PROFILES,
            null,
            null);
    assertEquals(all.getPageSize(), NUM_PROFILES);
    assertNull(all.getScrollId());
    Set<Long> expected =
        all.getDocuments().stream()
            .map(GenericTimeseriesDocument::getTimestampMillis)
            .collect(Collectors.toSet());
    assertEquals(expected.size(), NUM_PROFILES);

    Set<Long> seen = new HashSet<>();
    List<Long> ordered = new ArrayList<>();
    String scrollId = null;
    int pages = 0;
    while (true) {
      TimeseriesScrollResult page =
          pgTimeseries.scrollAspects(
              opContext,
              ENTITY_NAME,
              ASPECT_NAME,
              filter,
              sortCriteria,
              scrollId,
              pageSize,
              null,
              null);
      pages++;
      for (GenericTimeseriesDocument doc : page.getDocuments()) {
        assertTrue(
            seen.add(doc.getTimestampMillis()),
            "duplicate timestamp in scroll: " + doc.getTimestampMillis());
        ordered.add(doc.getTimestampMillis());
      }
      if (page.getScrollId() == null) {
        assertTrue(page.getPageSize() <= pageSize);
        break;
      }
      assertEquals(page.getPageSize(), pageSize);
      scrollId = page.getScrollId();
      assertTrue(pages < NUM_PROFILES, "scroll did not terminate");
    }

    assertEquals(seen, expected);
    assertEquals(
        ordered,
        all.getDocuments().stream()
            .map(GenericTimeseriesDocument::getTimestampMillis)
            .collect(Collectors.toList()));
    return ordered;
  }
}
