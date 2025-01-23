package com.linkedin.metadata.timeseries.search;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static io.datahubproject.test.search.SearchTestUtils.syncAfterWrite;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.datahub.test.BatchType;
import com.datahub.test.ComplexNestedRecord;
import com.datahub.test.TestEntityComponentProfile;
import com.datahub.test.TestEntityComponentProfileArray;
import com.datahub.test.TestEntityProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.template.StringMapArray;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.config.TimeseriesAspectServiceConfig;
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
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.TimeseriesAspectIndexBuilders;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.r2.RemoteInvocationException;
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
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public abstract class TimeseriesAspectServiceTestBase extends AbstractTestNGSpringContextTests {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  private static final String ENTITY_NAME = "testEntity";
  private static final String ASPECT_NAME = "testEntityProfile";
  private static final Urn TEST_URN =
      new TestEntityUrn("acryl", "testElasticSearchTimeseriesAspectService", "table1");
  private static final int NUM_PROFILES = 100;
  private static final long TIME_INCREMENT = 3600000; // hour in ms.
  private static final String CONTENT_TYPE = "application/json";

  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";
  private static final String ES_FIELD_STAT = "stat";

  @Nonnull
  protected abstract RestHighLevelClient getSearchClient();

  @Nonnull
  protected abstract ESBulkProcessor getBulkProcessor();

  @Nonnull
  protected abstract ESIndexBuilder getIndexBuilder();

  private OperationContext opContext;
  private ElasticSearchTimeseriesAspectService elasticSearchTimeseriesAspectService;
  private AspectSpec aspectSpec;

  private Map<Long, TestEntityProfile> testEntityProfiles;
  private Long startTime;

  /*
   * Basic setup and teardown
   */

  @BeforeClass
  public void setup() throws RemoteInvocationException, URISyntaxException {
    EntityRegistry entityRegistry =
        new ConfigEntityRegistry(
            new DataSchemaFactory("com.datahub.test"),
            List.of(),
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yml"));

    opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(
            entityRegistry,
            new IndexConventionImpl(
                IndexConventionImpl.IndexConventionConfig.builder()
                    .prefix("es_timeseries_aspect_service_test")
                    .hashIdAlgo("MD5")
                    .build()));

    elasticSearchTimeseriesAspectService = buildService();
    elasticSearchTimeseriesAspectService.reindexAll(Collections.emptySet());
    EntitySpec entitySpec = entityRegistry.getEntitySpec(ENTITY_NAME);
    aspectSpec = entitySpec.getAspectSpec(ASPECT_NAME);
  }

  @Nonnull
  private ElasticSearchTimeseriesAspectService buildService() {
    return new ElasticSearchTimeseriesAspectService(
        getSearchClient(),
        new TimeseriesAspectIndexBuilders(
            getIndexBuilder(),
            opContext.getEntityRegistry(),
            opContext.getSearchContext().getIndexConvention()),
        getBulkProcessor(),
        1,
        QueryFilterRewriteChain.EMPTY,
        TimeseriesAspectServiceConfig.builder().build());
  }

  /*
   * Tests for upsertDocument API
   */

  private void upsertDocument(TestEntityProfile dp, Urn urn) throws JsonProcessingException {
    Map<String, JsonNode> documents =
        TimeseriesAspectTransformer.transform(urn, dp, aspectSpec, null, "MD5");
    assertEquals(documents.size(), 3);
    documents.forEach(
        (key, value) ->
            elasticSearchTimeseriesAspectService.upsertDocument(
                opContext, ENTITY_NAME, ASPECT_NAME, key, value));
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

    // Add a couple of component profiles with cooked up stats.
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

  @Test(groups = "upsert")
  public void testUpsertProfiles() throws Exception {
    // Create the testEntity profiles that we would like to use for testing.
    startTime = Calendar.getInstance().getTimeInMillis();
    startTime = startTime - startTime % 86400000;
    // Create the testEntity profiles that we would like to use for testing.
    TestEntityProfile firstProfile = makeTestProfile(startTime, 20, null);
    Stream<TestEntityProfile> testEntityProfileStream =
        Stream.iterate(
            firstProfile,
            (TestEntityProfile prev) ->
                makeTestProfile(
                    prev.getTimestampMillis() + TIME_INCREMENT, prev.getStat() + 10, null));

    testEntityProfiles =
        testEntityProfileStream
            .limit(NUM_PROFILES)
            .collect(Collectors.toMap(TestEntityProfile::getTimestampMillis, Function.identity()));
    Long endTime = startTime + (NUM_PROFILES - 1) * TIME_INCREMENT;

    assertNotNull(testEntityProfiles.get(startTime));
    assertNotNull(testEntityProfiles.get(endTime));

    // Upsert the documents into the index.
    testEntityProfiles
        .values()
        .forEach(
            x -> {
              try {
                upsertDocument(x, TEST_URN);
              } catch (JsonProcessingException jsonProcessingException) {
                jsonProcessingException.printStackTrace();
              }
            });

    syncAfterWrite(getBulkProcessor());
  }

  @Test(groups = "upsertUniqueMessageId")
  public void testUpsertProfilesWithUniqueMessageIds() throws Exception {
    // Create the testEntity profiles that have the same value for timestampMillis, but use unique
    // message ids.
    // We should preserve all the documents we are going to upsert in the index.
    final long curTimeMillis = Calendar.getInstance().getTimeInMillis();
    final long startTime = curTimeMillis - curTimeMillis % 86400000;
    final TestEntityProfile firstProfile = makeTestProfile(startTime, 20, "20");
    Stream<TestEntityProfile> testEntityProfileStream =
        Stream.iterate(
            firstProfile,
            (TestEntityProfile prev) ->
                makeTestProfile(
                    prev.getTimestampMillis(),
                    prev.getStat() + 10,
                    String.valueOf(prev.getStat() + 10)));

    final List<TestEntityProfile> testEntityProfiles =
        testEntityProfileStream.limit(3).collect(Collectors.toList());

    // Upsert the documents into the index.
    final Urn urn =
        new TestEntityUrn("acryl", "testElasticSearchTimeseriesAspectService", "table2");
    testEntityProfiles.forEach(
        x -> {
          try {
            upsertDocument(x, urn);
          } catch (JsonProcessingException jsonProcessingException) {
            jsonProcessingException.printStackTrace();
          }
        });

    syncAfterWrite(getBulkProcessor());

    List<EnvelopedAspect> resultAspects =
        elasticSearchTimeseriesAspectService.getAspectValues(
            opContext, urn, ENTITY_NAME, ASPECT_NAME, null, null, testEntityProfiles.size(), null);
    assertEquals(resultAspects.size(), testEntityProfiles.size());
  }

  /*
   * Tests for getAspectValues API
   */

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

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesAll() {
    List<EnvelopedAspect> resultAspects =
        elasticSearchTimeseriesAspectService.getAspectValues(
            opContext, TEST_URN, ENTITY_NAME, ASPECT_NAME, null, null, NUM_PROFILES, null);
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

    // Now verify that the first index is the one with the highest stat value, and the last the one
    // with the lower.
    assertEquals((long) firstProfile.getStat(), 20 + (NUM_PROFILES - 1) * 10);
    assertEquals((long) lastProfile.getStat(), 20);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesAllSorted() {
    List<EnvelopedAspect> resultAspects =
        elasticSearchTimeseriesAspectService.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            null,
            null,
            NUM_PROFILES,
            null,
            new SortCriterion().setField("stat").setOrder(SortOrder.ASCENDING));
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

    // Now verify that the first index is the one with the highest stat value, and the last the one
    // with the lower.
    assertEquals((long) firstProfile.getStat(), 20);
    assertEquals((long) lastProfile.getStat(), 20 + (NUM_PROFILES - 1) * 10);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesWithFilter() {
    Filter filter = new Filter();
    Criterion hasStatEqualsTwenty = buildCriterion("stat", Condition.EQUAL, "20");
    filter.setCriteria(new CriterionArray(hasStatEqualsTwenty));
    List<EnvelopedAspect> resultAspects =
        elasticSearchTimeseriesAspectService.getAspectValues(
            opContext, TEST_URN, ENTITY_NAME, ASPECT_NAME, null, null, NUM_PROFILES, filter);
    validateAspectValues(resultAspects, 1);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesSubRangeInclusiveOverlap() {
    int expectedNumRows = 10;
    List<EnvelopedAspect> resultAspects =
        elasticSearchTimeseriesAspectService.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            startTime,
            startTime + TIME_INCREMENT * (expectedNumRows - 1),
            expectedNumRows,
            null);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesSubRangeExclusiveOverlap() {
    int expectedNumRows = 10;
    List<EnvelopedAspect> resultAspects =
        elasticSearchTimeseriesAspectService.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            startTime + TIME_INCREMENT / 2,
            startTime + TIME_INCREMENT * expectedNumRows + TIME_INCREMENT / 2,
            expectedNumRows,
            null);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesSubRangeExclusiveOverlapLatestValueOnly() {
    int expectedNumRows = 1;
    List<EnvelopedAspect> resultAspects =
        elasticSearchTimeseriesAspectService.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            startTime + TIME_INCREMENT / 2,
            startTime + TIME_INCREMENT * expectedNumRows + TIME_INCREMENT / 2,
            expectedNumRows,
            null);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesExactlyOneResponse() {
    int expectedNumRows = 1;
    List<EnvelopedAspect> resultAspects =
        elasticSearchTimeseriesAspectService.getAspectValues(
            opContext,
            TEST_URN,
            ENTITY_NAME,
            ASPECT_NAME,
            startTime + TIME_INCREMENT / 2,
            startTime + TIME_INCREMENT * 3 / 2,
            expectedNumRows,
            null);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(
      groups = {"getAspectValues"},
      dependsOnGroups = {"upsert"})
  public void testGetAspectTimeseriesValueMissingUrn() {
    Urn nonExistingUrn = new TestEntityUrn("missing", "missing", "missing");
    List<EnvelopedAspect> resultAspects =
        elasticSearchTimeseriesAspectService.getAspectValues(
            opContext, nonExistingUrn, ENTITY_NAME, ASPECT_NAME, null, null, NUM_PROFILES, null);
    validateAspectValues(resultAspects, 0);
  }

  /*
   * Tests for getAggregatedStats API
   */

  /* Latest Aggregation Tests */
  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.START_WITH, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {latestStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray(ES_FIELD_TIMESTAMP, "latest_" + ES_FIELD_STAT));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(
        resultTable.getRows(),
        new StringArrayArray(
            new StringArray(
                startTime.toString(),
                testEntityProfiles.get(startTime + 23 * TIME_INCREMENT).getStat().toString())));
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForDay1WithValues() {
    // Filter is only on the urn
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {latestStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray(ES_FIELD_TIMESTAMP, "latest_" + ES_FIELD_STAT));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(
        resultTable.getRows(),
        new StringArrayArray(
            new StringArray(
                startTime.toString(),
                testEntityProfiles.get(startTime + 23 * TIME_INCREMENT).getStat().toString())));
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestAComplexNestedRecordForDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.LATEST)
            .setFieldPath("aComplexNestedRecord");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {latestStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray(ES_FIELD_TIMESTAMP, "latest_aComplexNestedRecord"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "record"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows().get(0).get(0), startTime.toString());
    try {
      ComplexNestedRecord latestAComplexNestedRecord =
          OBJECT_MAPPER.readValue(resultTable.getRows().get(0).get(1), ComplexNestedRecord.class);
      assertEquals(
          latestAComplexNestedRecord,
          testEntityProfiles.get(startTime + 23 * TIME_INCREMENT).getAComplexNestedRecord());
    } catch (JsonProcessingException e) {
      fail("Unexpected exception thrown" + e);
    }
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStrArrayDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("strArray");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {latestStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(), new StringArray(ES_FIELD_TIMESTAMP, "latest_" + "strArray"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "array"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    StringArray expectedStrArray =
        testEntityProfiles.get(startTime + 23 * TIME_INCREMENT).getStrArray();
    // assertEquals(resultTable.getRows(), new StringArrayArray(new
    // StringArray(_startTime.toString(),
    //    expectedStrArray.toString())));
    // Test array construction using object mapper as well
    try {
      StringArray actualStrArray =
          OBJECT_MAPPER.readValue(resultTable.getRows().get(0).get(1), StringArray.class);
      assertEquals(actualStrArray, expectedStrArray);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForTwoDays() {
    // Filter is only on the urn
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 47 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {latestStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray(ES_FIELD_TIMESTAMP, "latest_" + ES_FIELD_STAT));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 2);
    Long latestDay1Ts = startTime + 23 * TIME_INCREMENT;
    Long latestDay2Ts = startTime + 47 * TIME_INCREMENT;
    assertEquals(
        resultTable.getRows(),
        new StringArrayArray(
            new StringArray(
                startTime.toString(), testEntityProfiles.get(latestDay1Ts).getStat().toString()),
            new StringArray(
                String.valueOf(startTime + 24 * TIME_INCREMENT),
                testEntityProfiles.get(latestDay2Ts).getStat().toString())));
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForFirst10HoursOfDay1() {
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 9 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {latestStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray(ES_FIELD_TIMESTAMP, "latest_" + ES_FIELD_STAT));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(
        resultTable.getRows(),
        new StringArrayArray(
            new StringArray(
                startTime.toString(),
                testEntityProfiles.get(startTime + 9 * TIME_INCREMENT).getStat().toString())));
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForCol1Day1() {
    Long lastEntryTimeStamp = startTime + 23 * TIME_INCREMENT;
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(lastEntryTimeStamp));

    Criterion hasCol1 = buildCriterion("componentProfiles.key", Condition.EQUAL, "col1");

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, hasCol1, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.LATEST)
            .setFieldPath("componentProfiles.stat");

    // Grouping bucket is timestamp filed + componentProfiles.key.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GroupingBucket componentProfilesBucket =
        new GroupingBucket()
            .setKey("componentProfiles.key")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {latestStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket, componentProfilesBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray(
            ES_FIELD_TIMESTAMP, "componentProfiles.key", "latest_" + "componentProfiles.stat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "string", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(
        resultTable.getRows(),
        new StringArrayArray(
            new StringArray(
                startTime.toString(),
                "col1",
                testEntityProfiles
                    .get(lastEntryTimeStamp)
                    .getComponentProfiles()
                    .get(0)
                    .getStat()
                    .toString())));
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForAllColumnsDay1() {
    Long lastEntryTimeStamp = startTime + 23 * TIME_INCREMENT;
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());

    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());

    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(lastEntryTimeStamp));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.LATEST)
            .setFieldPath("componentProfiles.stat");

    // Grouping bucket is timestamp filed + componentProfiles.key.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GroupingBucket componentProfilesBucket =
        new GroupingBucket()
            .setKey("componentProfiles.key")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {latestStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket, componentProfilesBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray(
            ES_FIELD_TIMESTAMP, "componentProfiles.key", "latest_" + "componentProfiles.stat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "string", "long"));
    // Validate rows
    StringArray expectedRow1 =
        new StringArray(
            startTime.toString(),
            "col1",
            testEntityProfiles
                .get(lastEntryTimeStamp)
                .getComponentProfiles()
                .get(0)
                .getStat()
                .toString());
    StringArray expectedRow2 =
        new StringArray(
            startTime.toString(),
            "col2",
            testEntityProfiles
                .get(lastEntryTimeStamp)
                .getComponentProfiles()
                .get(1)
                .getStat()
                .toString());

    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 2);
    assertEquals(resultTable.getRows(), new StringArrayArray(expectedRow1, expectedRow2));
  }

  /* Sum Aggregation Tests */
  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsSumStatForFirst10HoursOfDay1() {
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 9 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate the sum of stat value
    AggregationSpec sumAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {sumAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(), new StringArray(ES_FIELD_TIMESTAMP, "sum_" + ES_FIELD_STAT));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "double"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    // value is 20+30+40+... up to 10 terms = 650
    // TODO: Compute this caching the documents.
    assertEquals(
        resultTable.getRows(),
        new StringArrayArray(new StringArray(startTime.toString(), String.valueOf(650))));
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsSumStatForCol2Day1() {
    Long lastEntryTimeStamp = startTime + 23 * TIME_INCREMENT;
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());

    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());

    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(lastEntryTimeStamp));

    Criterion hasCol2 = buildCriterion("componentProfiles.key", Condition.EQUAL, "col2");

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, hasCol2, startTimeCriterion, endTimeCriterion));

    // Aggregate the sum of stat value
    AggregationSpec sumStatAggregationSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.SUM)
            .setFieldPath("componentProfiles.stat");

    // Grouping bucket is timestamp filed + componentProfiles.key.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GroupingBucket componentProfilesBucket =
        new GroupingBucket()
            .setKey("componentProfiles.key")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {sumStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket, componentProfilesBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray(
            ES_FIELD_TIMESTAMP, "componentProfiles.key", "sum_" + "componentProfiles.stat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "string", "double"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    // value = 22+32+42+... 24 terms = 3288
    // TODO: Compute this caching the documents.
    assertEquals(
        resultTable.getRows(),
        new StringArrayArray(new StringArray(startTime.toString(), "col2", String.valueOf(3288))));
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsCardinalityAggStrStatDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec cardinalityStatAggregationSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.CARDINALITY)
            .setFieldPath("strStat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket =
        new GroupingBucket()
            .setKey(ES_FIELD_TIMESTAMP)
            .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
            .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {cardinalityStatAggregationSpec},
            filter,
            new GroupingBucket[] {timestampBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray(ES_FIELD_TIMESTAMP, "cardinality_" + "strStat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(
        resultTable.getRows(), new StringArrayArray(new StringArray(startTime.toString(), "24")));
  }

  @Test(
      groups = {"getAggregatedStats", "usageStats"},
      dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsSumStatsCollectionDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec cardinalityStatAggregationSpec =
        new AggregationSpec()
            .setAggregationType(AggregationType.SUM)
            .setFieldPath("componentProfiles.stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket profileStatBucket =
        new GroupingBucket()
            .setKey("componentProfiles.key")
            .setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GenericTable resultTable =
        elasticSearchTimeseriesAspectService.getAggregatedStats(
            opContext,
            ENTITY_NAME,
            ASPECT_NAME,
            new AggregationSpec[] {cardinalityStatAggregationSpec},
            filter,
            new GroupingBucket[] {profileStatBucket});
    // Validate column names
    assertEquals(
        resultTable.getColumnNames(),
        new StringArray("componentProfiles.key", "sum_" + "componentProfiles.stat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("string", "double"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 2);
    assertEquals(
        resultTable.getRows(),
        new StringArrayArray(new StringArray("col1", "3264"), new StringArray("col2", "3288")));
  }

  @Test(
      groups = {"deleteAspectValues1"},
      dependsOnGroups = {"getAggregatedStats", "getAspectValues", "testCountBeforeDelete"})
  public void testDeleteAspectValuesByUrnAndTimeRangeDay1() {
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());
    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));
    DeleteAspectValuesResult result =
        elasticSearchTimeseriesAspectService.deleteAspectValues(
            opContext, ENTITY_NAME, ASPECT_NAME, filter);
    // For day1, we expect 24 (number of hours) * 3 (each testEntityProfile aspect expands 3 elastic
    // docs:
    //  1 original + 2 for componentProfiles) = 72 total.
    assertEquals(result.getNumDocsDeleted(), Long.valueOf(72L));
  }

  @Test(
      groups = {"deleteAspectValues2"},
      dependsOnGroups = {"deleteAspectValues1", "testCountAfterDelete"})
  public void testDeleteAspectValuesByUrn() {
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());

    Filter filter = QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion));
    DeleteAspectValuesResult result =
        elasticSearchTimeseriesAspectService.deleteAspectValues(
            opContext, ENTITY_NAME, ASPECT_NAME, filter);
    // Of the 300 elastic docs upserted for TEST_URN, 72 got deleted by deleteAspectValues1 test
    // group leaving 228.
    assertEquals(result.getNumDocsDeleted(), Long.valueOf(228L));
  }

  @Test(
      groups = {"testCountBeforeDelete"},
      dependsOnGroups = {"upsert"})
  public void testCountByFilter() {
    // Test with filter
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());

    Filter filter = QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion));
    long count =
        elasticSearchTimeseriesAspectService.countByFilter(
            opContext, ENTITY_NAME, ASPECT_NAME, filter);
    assertEquals(count, 300L);

    // Test with filter with multiple criteria
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());

    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 23 * TIME_INCREMENT));

    Filter urnAndTimeFilter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));
    count =
        elasticSearchTimeseriesAspectService.countByFilter(
            opContext, ENTITY_NAME, ASPECT_NAME, urnAndTimeFilter);
    assertEquals(count, 72L);

    // test without filter
    count =
        elasticSearchTimeseriesAspectService.countByFilter(
            opContext, ENTITY_NAME, ASPECT_NAME, new Filter());
    // There may be other entities in there from other tests
    assertTrue(count >= 300L);
  }

  @Test(
      groups = {"testCountAfterDelete"},
      dependsOnGroups = {"deleteAspectValues1"})
  public void testCountByFilterAfterDelete() throws Exception {
    syncAfterWrite(getBulkProcessor());
    // Test with filter
    Criterion hasUrnCriterion = buildCriterion("urn", Condition.EQUAL, TEST_URN.toString());

    Filter filter = QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion));
    long count =
        elasticSearchTimeseriesAspectService.countByFilter(
            opContext, ENTITY_NAME, ASPECT_NAME, filter);
    assertEquals(count, 228L);

    // Test with filter with multiple criteria
    Criterion startTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTime.toString());

    Criterion endTimeCriterion =
        buildCriterion(
            ES_FIELD_TIMESTAMP,
            Condition.LESS_THAN_OR_EQUAL_TO,
            String.valueOf(startTime + 23 * TIME_INCREMENT));

    Filter urnAndTimeFilter =
        QueryUtils.getFilterFromCriteria(
            ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));
    count =
        elasticSearchTimeseriesAspectService.countByFilter(
            opContext, ENTITY_NAME, ASPECT_NAME, urnAndTimeFilter);
    assertEquals(count, 0L);
  }

  @Test(
      groups = {"getAggregatedStats"},
      dependsOnGroups = {"upsert"})
  public void testGetIndexSizes() {
    List<TimeseriesIndexSizeResult> result =
        elasticSearchTimeseriesAspectService.getIndexSizes(opContext);
    // CHECKSTYLE:OFF
    /*
    Example result:
    {aspectName=testentityprofile, sizeMb=52.234,
    indexName=es_timeseries_aspect_service_test_testentity_testentityprofileaspect_v1, entityName=testentity}
    {aspectName=testentityprofile, sizeMb=0.208,
    indexName=es_timeseries_aspect_service_test_testentitywithouttests_testentityprofileaspect_v1, entityName=testentitywithouttests}
     */
    // There may be other indices in there from other tests, so just make sure that index for entity
    // + aspect is in there
    // CHECKSTYLE:ON
    assertTrue(result.size() > 0);
    assertTrue(
        result.stream()
            .anyMatch(
                idxSizeResult ->
                    idxSizeResult
                        .getIndexName()
                        .equals(
                            "es_timeseries_aspect_service_test_testentity_testentityprofileaspect_v1")));
  }
}
