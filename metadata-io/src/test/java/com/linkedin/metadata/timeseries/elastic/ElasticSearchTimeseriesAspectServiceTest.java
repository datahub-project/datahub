package com.linkedin.metadata.timeseries.elastic;

import com.datahub.test.BatchType;
import com.datahub.test.ComplexNestedRecord;
import com.datahub.test.TestEntityComponentProfile;
import com.datahub.test.TestEntityComponentProfileArray;
import com.datahub.test.TestEntityProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.data.template.StringMapArray;
import com.linkedin.metadata.ElasticSearchTestConfiguration;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.TimeseriesAspectIndexBuilders;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
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
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.metadata.ElasticSearchTestConfiguration.syncAfterWrite;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Import(ElasticSearchTestConfiguration.class)
public class ElasticSearchTimeseriesAspectServiceTest extends AbstractTestNGSpringContextTests {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String ENTITY_NAME = "testEntity";
  private static final String ASPECT_NAME = "testEntityProfile";
  private static final Urn TEST_URN = new TestEntityUrn("acryl", "testElasticSearchTimeseriesAspectService", "table1");
  private static final int NUM_PROFILES = 100;
  private static final long TIME_INCREMENT = 3600000; // hour in ms.
  private static final String CONTENT_TYPE = "application/json";

  private static final String ES_FILED_TIMESTAMP = "timestampMillis";
  private static final String ES_FILED_STAT = "stat";

  @Autowired
  private RestHighLevelClient _searchClient;
  @Autowired
  private ESBulkProcessor _bulkProcessor;
  @Autowired
  private ESIndexBuilder _esIndexBuilder;
  private EntityRegistry _entityRegistry;
  private IndexConvention _indexConvention;
  private ElasticSearchTimeseriesAspectService _elasticSearchTimeseriesAspectService;
  private AspectSpec _aspectSpec;

  private Map<Long, TestEntityProfile> _testEntityProfiles;
  private Long _startTime;

  /*
   * Basic setup and teardown
   */

  @BeforeClass
  public void setup() {
    _entityRegistry = new ConfigEntityRegistry(new DataSchemaFactory("com.datahub.test"),
        TestEntityProfile.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
    _indexConvention = new IndexConventionImpl("es_timeseries_aspect_service_test");
    _elasticSearchTimeseriesAspectService = buildService();
    _elasticSearchTimeseriesAspectService.configure();
    EntitySpec entitySpec = _entityRegistry.getEntitySpec(ENTITY_NAME);
    _aspectSpec = entitySpec.getAspectSpec(ASPECT_NAME);
  }

  @Nonnull
  private ElasticSearchTimeseriesAspectService buildService() {
    return new ElasticSearchTimeseriesAspectService(_searchClient, _indexConvention,
        new TimeseriesAspectIndexBuilders(_esIndexBuilder, _entityRegistry,
            _indexConvention), _entityRegistry, _bulkProcessor, 1);
  }

  /*
   * Tests for upsertDocument API
   */

  private void upsertDocument(TestEntityProfile dp, Urn urn) throws JsonProcessingException {
    Map<String, JsonNode> documents = TimeseriesAspectTransformer.transform(urn, dp, _aspectSpec, null);
    assertEquals(documents.size(), 3);
    documents.forEach(
        (key, value) -> _elasticSearchTimeseriesAspectService.upsertDocument(ENTITY_NAME, ASPECT_NAME, key, value));
  }

  private TestEntityProfile makeTestProfile(long eventTime, long stat, String messageId) {
    TestEntityProfile testEntityProfile = new TestEntityProfile();
    testEntityProfile.setTimestampMillis(eventTime);
    testEntityProfile.setStat(stat);
    testEntityProfile.setStrStat(String.valueOf(stat));
    testEntityProfile.setStrArray(new StringArray("sa_" + stat, "sa_" + (stat + 1)));
    testEntityProfile.setEventGranularity(new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(1));
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
    testEntityProfile.setComponentProfiles(new TestEntityComponentProfileArray(componentProfile1, componentProfile2));

    StringMap stringMap1 = new StringMap();
    stringMap1.put("p_key1", "p_val1");
    StringMap stringMap2 = new StringMap();
    stringMap2.put("p_key2", "p_val2");
    ComplexNestedRecord nestedRecord = new ComplexNestedRecord().setType(BatchType.PARTITION_BATCH)
        .setPartitions(new StringMapArray(stringMap1, stringMap2));
    testEntityProfile.setAComplexNestedRecord(nestedRecord);

    return testEntityProfile;
  }

  @Test(groups = "upsert")
  public void testUpsertProfiles() throws Exception {
    // Create the testEntity profiles that we would like to use for testing.
    _startTime = Calendar.getInstance().getTimeInMillis();
    _startTime = _startTime - _startTime % 86400000;
    // Create the testEntity profiles that we would like to use for testing.
    TestEntityProfile firstProfile = makeTestProfile(_startTime, 20, null);
    Stream<TestEntityProfile> testEntityProfileStream = Stream.iterate(firstProfile,
        (TestEntityProfile prev) -> makeTestProfile(prev.getTimestampMillis() + TIME_INCREMENT, prev.getStat() + 10,
            null));

    _testEntityProfiles = testEntityProfileStream.limit(NUM_PROFILES)
        .collect(Collectors.toMap(TestEntityProfile::getTimestampMillis, Function.identity()));
    Long endTime = _startTime + (NUM_PROFILES - 1) * TIME_INCREMENT;

    assertNotNull(_testEntityProfiles.get(_startTime));
    assertNotNull(_testEntityProfiles.get(endTime));

    // Upsert the documents into the index.
    _testEntityProfiles.values().forEach(x -> {
      try {
        upsertDocument(x, TEST_URN);
      } catch (JsonProcessingException jsonProcessingException) {
        jsonProcessingException.printStackTrace();
      }
    });

    syncAfterWrite();
  }

  @Test(groups = "upsertUniqueMessageId")
  public void testUpsertProfilesWithUniqueMessageIds() throws Exception {
    // Create the testEntity profiles that have the same value for timestampMillis, but use unique message ids.
    // We should preserve all the documents we are going to upsert in the index.
    final long curTimeMillis = Calendar.getInstance().getTimeInMillis();
    final long startTime = curTimeMillis - curTimeMillis % 86400000;
    final TestEntityProfile firstProfile = makeTestProfile(startTime, 20, "20");
    Stream<TestEntityProfile> testEntityProfileStream = Stream.iterate(firstProfile,
        (TestEntityProfile prev) -> makeTestProfile(prev.getTimestampMillis(), prev.getStat() + 10,
            String.valueOf(prev.getStat() + 10)));

    final List<TestEntityProfile> testEntityProfiles = testEntityProfileStream.limit(3).collect(Collectors.toList());

    // Upsert the documents into the index.
    final Urn urn = new TestEntityUrn("acryl", "testElasticSearchTimeseriesAspectService", "table2");
    testEntityProfiles.forEach(x -> {
      try {
        upsertDocument(x, urn);
      } catch (JsonProcessingException jsonProcessingException) {
        jsonProcessingException.printStackTrace();
      }
    });

    syncAfterWrite();

    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(urn, ENTITY_NAME, ASPECT_NAME, null, null,
            testEntityProfiles.size(), false, null);
    assertEquals(resultAspects.size(), testEntityProfiles.size());
  }

  /*
   * Tests for getAspectValues API
   */

  private void validateAspectValue(EnvelopedAspect envelopedAspectResult) {
    TestEntityProfile actualProfile =
        (TestEntityProfile) GenericRecordUtils.deserializeAspect(envelopedAspectResult.getAspect().getValue(),
            CONTENT_TYPE, _aspectSpec);
    TestEntityProfile expectedProfile = _testEntityProfiles.get(actualProfile.getTimestampMillis());
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
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME, null, null,
            NUM_PROFILES, false, null);
    validateAspectValues(resultAspects, NUM_PROFILES);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesWithFilter() {
    Filter filter = new Filter();
    Criterion hasStatEqualsTwenty = new Criterion().setField("stat").setCondition(Condition.EQUAL).setValue("20");
    filter.setCriteria(new CriterionArray(hasStatEqualsTwenty));
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME, null, null,
            NUM_PROFILES, false, filter);
    validateAspectValues(resultAspects, 1);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesSubRangeInclusiveOverlap() {
    int expectedNumRows = 10;
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME, _startTime,
            _startTime + TIME_INCREMENT * (expectedNumRows - 1), expectedNumRows, false, null);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesSubRangeExclusiveOverlap() {
    int expectedNumRows = 10;
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME,
            _startTime + TIME_INCREMENT / 2, _startTime + TIME_INCREMENT * expectedNumRows + TIME_INCREMENT / 2,
            expectedNumRows, false, null);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesSubRangeExclusiveOverlapLatestValueOnly() {
    int expectedNumRows = 1;
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME,
            _startTime + TIME_INCREMENT / 2, _startTime + TIME_INCREMENT * expectedNumRows + TIME_INCREMENT / 2,
            expectedNumRows, true, null);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = "getAspectValues", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesExactlyOneResponse() {
    int expectedNumRows = 1;
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME,
            _startTime + TIME_INCREMENT / 2, _startTime + TIME_INCREMENT * 3 / 2, expectedNumRows, false, null);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = {"getAspectValues"}, dependsOnGroups = {"upsert"})
  public void testGetAspectTimeseriesValueMissingUrn() {
    Urn nonExistingUrn = new TestEntityUrn("missing", "missing", "missing");
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(nonExistingUrn, ENTITY_NAME, ASPECT_NAME, null, null,
            NUM_PROFILES, false, null);
    validateAspectValues(resultAspects, 0);
  }

  /*
   * Tests for getAggregatedStats API
   */

  /* Latest Aggregation Tests */
  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(_startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{latestStatAggregationSpec}, filter, new GroupingBucket[]{timestampBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(), new StringArray(ES_FILED_TIMESTAMP, "latest_" + ES_FILED_STAT));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows(), new StringArrayArray(new StringArray(_startTime.toString(),
        _testEntityProfiles.get(_startTime + 23 * TIME_INCREMENT).getStat().toString())));
  }

  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestAComplexNestedRecordForDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(_startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("aComplexNestedRecord");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{latestStatAggregationSpec}, filter, new GroupingBucket[]{timestampBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(), new StringArray(ES_FILED_TIMESTAMP, "latest_aComplexNestedRecord"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "record"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows().get(0).get(0), _startTime.toString());
    try {
      ComplexNestedRecord latestAComplexNestedRecord =
          OBJECT_MAPPER.readValue(resultTable.getRows().get(0).get(1), ComplexNestedRecord.class);
      assertEquals(latestAComplexNestedRecord,
          _testEntityProfiles.get(_startTime + 23 * TIME_INCREMENT).getAComplexNestedRecord());
    } catch (JsonProcessingException e) {
      fail("Unexpected exception thrown" + e);
    }
  }

  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStrArrayDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(_startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("strArray");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{latestStatAggregationSpec}, filter, new GroupingBucket[]{timestampBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(), new StringArray(ES_FILED_TIMESTAMP, "latest_" + "strArray"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "array"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    StringArray expectedStrArray = _testEntityProfiles.get(_startTime + 23 * TIME_INCREMENT).getStrArray();
    //assertEquals(resultTable.getRows(), new StringArrayArray(new StringArray(_startTime.toString(),
    //    expectedStrArray.toString())));
    // Test array construction using object mapper as well
    try {
      StringArray actualStrArray = OBJECT_MAPPER.readValue(resultTable.getRows().get(0).get(1), StringArray.class);
      assertEquals(actualStrArray, expectedStrArray);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForTwoDays() {
    // Filter is only on the urn
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(_startTime + 47 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{latestStatAggregationSpec}, filter, new GroupingBucket[]{timestampBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(), new StringArray(ES_FILED_TIMESTAMP, "latest_" + ES_FILED_STAT));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 2);
    Long latestDay1Ts = _startTime + 23 * TIME_INCREMENT;
    Long latestDay2Ts = _startTime + 47 * TIME_INCREMENT;
    assertEquals(resultTable.getRows(), new StringArrayArray(
        new StringArray(_startTime.toString(), _testEntityProfiles.get(latestDay1Ts).getStat().toString()),
        new StringArray(String.valueOf(_startTime + 24 * TIME_INCREMENT),
            _testEntityProfiles.get(latestDay2Ts).getStat().toString())));
  }

  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForFirst10HoursOfDay1() {
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(_startTime + 9 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{latestStatAggregationSpec}, filter, new GroupingBucket[]{timestampBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(), new StringArray(ES_FILED_TIMESTAMP, "latest_" + ES_FILED_STAT));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows(), new StringArrayArray(new StringArray(_startTime.toString(),
        _testEntityProfiles.get(_startTime + 9 * TIME_INCREMENT).getStat().toString())));
  }

  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForCol1Day1() {
    Long lastEntryTimeStamp = _startTime + 23 * TIME_INCREMENT;
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(lastEntryTimeStamp));
    Criterion hasCol1 =
        new Criterion().setField("componentProfiles.key").setCondition(Condition.EQUAL).setValue("col1");

    Filter filter = QueryUtils.getFilterFromCriteria(
        ImmutableList.of(hasUrnCriterion, hasCol1, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("componentProfiles.stat");

    // Grouping bucket is timestamp filed + componentProfiles.key.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GroupingBucket componentProfilesBucket =
        new GroupingBucket().setKey("componentProfiles.key").setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{latestStatAggregationSpec}, filter,
        new GroupingBucket[]{timestampBucket, componentProfilesBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(),
        new StringArray(ES_FILED_TIMESTAMP, "componentProfiles.key", "latest_" + "componentProfiles.stat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "string", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows(), new StringArrayArray(new StringArray(_startTime.toString(), "col1",
        _testEntityProfiles.get(lastEntryTimeStamp).getComponentProfiles().get(0).getStat().toString())));
  }

  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsLatestStatForAllColumnsDay1() {
    Long lastEntryTimeStamp = _startTime + 23 * TIME_INCREMENT;
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(lastEntryTimeStamp));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec latestStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.LATEST).setFieldPath("componentProfiles.stat");

    // Grouping bucket is timestamp filed + componentProfiles.key.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GroupingBucket componentProfilesBucket =
        new GroupingBucket().setKey("componentProfiles.key").setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{latestStatAggregationSpec}, filter,
        new GroupingBucket[]{timestampBucket, componentProfilesBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(),
        new StringArray(ES_FILED_TIMESTAMP, "componentProfiles.key", "latest_" + "componentProfiles.stat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "string", "long"));
    // Validate rows
    StringArray expectedRow1 = new StringArray(_startTime.toString(), "col1",
        _testEntityProfiles.get(lastEntryTimeStamp).getComponentProfiles().get(0).getStat().toString());
    StringArray expectedRow2 = new StringArray(_startTime.toString(), "col2",
        _testEntityProfiles.get(lastEntryTimeStamp).getComponentProfiles().get(1).getStat().toString());

    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 2);
    assertEquals(resultTable.getRows(), new StringArrayArray(expectedRow1, expectedRow2));
  }

  /* Sum Aggregation Tests */
  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsSumStatForFirst10HoursOfDay1() {
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(_startTime + 9 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate the sum of stat value
    AggregationSpec sumAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{sumAggregationSpec}, filter, new GroupingBucket[]{timestampBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(), new StringArray(ES_FILED_TIMESTAMP, "sum_" + ES_FILED_STAT));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "double"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    // value is 20+30+40+... up to 10 terms = 650
    // TODO: Compute this caching the documents.
    assertEquals(resultTable.getRows(),
        new StringArrayArray(new StringArray(_startTime.toString(), String.valueOf(650))));
  }

  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsSumStatForCol2Day1() {
    Long lastEntryTimeStamp = _startTime + 23 * TIME_INCREMENT;
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(lastEntryTimeStamp));
    Criterion hasCol2 =
        new Criterion().setField("componentProfiles.key").setCondition(Condition.EQUAL).setValue("col2");

    Filter filter = QueryUtils.getFilterFromCriteria(
        ImmutableList.of(hasUrnCriterion, hasCol2, startTimeCriterion, endTimeCriterion));

    // Aggregate the sum of stat value
    AggregationSpec sumStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("componentProfiles.stat");

    // Grouping bucket is timestamp filed + componentProfiles.key.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GroupingBucket componentProfilesBucket =
        new GroupingBucket().setKey("componentProfiles.key").setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{sumStatAggregationSpec}, filter,
        new GroupingBucket[]{timestampBucket, componentProfilesBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(),
        new StringArray(ES_FILED_TIMESTAMP, "componentProfiles.key", "sum_" + "componentProfiles.stat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "string", "double"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    // value = 22+32+42+... 24 terms = 3288
    // TODO: Compute this caching the documents.
    assertEquals(resultTable.getRows(),
        new StringArrayArray(new StringArray(_startTime.toString(), "col2", String.valueOf(3288))));
  }

  @Test(groups = {"getAggregatedStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsCardinalityAggStrStatDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(_startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec cardinalityStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.CARDINALITY).setFieldPath("strStat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket timestampBucket = new GroupingBucket().setKey(ES_FILED_TIMESTAMP)
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(new TimeWindowSize().setMultiple(1).setUnit(CalendarInterval.DAY));

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{cardinalityStatAggregationSpec}, filter, new GroupingBucket[]{timestampBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(), new StringArray(ES_FILED_TIMESTAMP, "cardinality_" + "strStat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("long", "long"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows(), new StringArrayArray(new StringArray(_startTime.toString(), "24")));
  }

  @Test(groups = {"getAggregatedStats", "usageStats"}, dependsOnGroups = {"upsert"})
  public void testGetAggregatedStatsSumStatsCollectionDay1() {
    // Filter is only on the urn
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(_startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));

    // Aggregate on latest stat value
    AggregationSpec cardinalityStatAggregationSpec =
        new AggregationSpec().setAggregationType(AggregationType.SUM).setFieldPath("componentProfiles.stat");

    // Grouping bucket is only timestamp filed.
    GroupingBucket profileStatBucket =
        new GroupingBucket().setKey("componentProfiles.key").setType(GroupingBucketType.STRING_GROUPING_BUCKET);

    GenericTable resultTable = _elasticSearchTimeseriesAspectService.getAggregatedStats(ENTITY_NAME, ASPECT_NAME,
        new AggregationSpec[]{cardinalityStatAggregationSpec}, filter, new GroupingBucket[]{profileStatBucket});
    // Validate column names
    assertEquals(resultTable.getColumnNames(),
        new StringArray("componentProfiles.key", "sum_" + "componentProfiles.stat"));
    // Validate column types
    assertEquals(resultTable.getColumnTypes(), new StringArray("string", "double"));
    // Validate rows
    assertNotNull(resultTable.getRows());
    assertEquals(resultTable.getRows().size(), 2);
    assertEquals(resultTable.getRows(),
        new StringArrayArray(new StringArray("col1", "3264"), new StringArray("col2", "3288")));
  }

  @Test(groups = {"deleteAspectValues1"}, dependsOnGroups = {"getAggregatedStats", "getAspectValues"})
  public void testDeleteAspectValuesByUrnAndTimeRangeDay1() {
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Criterion startTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.GREATER_THAN_OR_EQUAL_TO)
        .setValue(_startTime.toString());
    Criterion endTimeCriterion = new Criterion().setField(ES_FILED_TIMESTAMP)
        .setCondition(Condition.LESS_THAN_OR_EQUAL_TO)
        .setValue(String.valueOf(_startTime + 23 * TIME_INCREMENT));

    Filter filter =
        QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion, startTimeCriterion, endTimeCriterion));
    DeleteAspectValuesResult result =
        _elasticSearchTimeseriesAspectService.deleteAspectValues(ENTITY_NAME, ASPECT_NAME, filter);
    // For day1, we expect 24 (number of hours) * 3 (each testEntityProfile aspect expands 3 elastic docs:
    //  1 original + 2 for componentProfiles) = 72 total.
    assertEquals(result.getNumDocsDeleted(), Long.valueOf(72L));
  }

  @Test(groups = {"deleteAspectValues2"}, dependsOnGroups = {"deleteAspectValues1"})
  public void testDeleteAspectValuesByUrn() {
    Criterion hasUrnCriterion =
        new Criterion().setField("urn").setCondition(Condition.EQUAL).setValue(TEST_URN.toString());
    Filter filter = QueryUtils.getFilterFromCriteria(ImmutableList.of(hasUrnCriterion));
    DeleteAspectValuesResult result =
        _elasticSearchTimeseriesAspectService.deleteAspectValues(ENTITY_NAME, ASPECT_NAME, filter);
    // Of the 300 elastic docs upserted for TEST_URN, 72 got deleted by deleteAspectValues1 test group leaving 228.
    assertEquals(result.getNumDocsDeleted(), Long.valueOf(228L));
  }
}
