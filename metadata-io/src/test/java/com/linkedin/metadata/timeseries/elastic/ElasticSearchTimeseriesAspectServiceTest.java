package com.linkedin.metadata.timeseries.elastic;

import com.datahub.test.TestEntityProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.timeseries.elastic.indexbuilder.TimeseriesAspectIndexBuilders;
import com.linkedin.metadata.timeseries.transformer.TimeseriesAspectTransformer;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

import static com.linkedin.metadata.ElasticSearchTestUtils.syncAfterWrite;

public class ElasticSearchTimeseriesAspectServiceTest {

  private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:7.9.3";
  private static final int HTTP_PORT = 9200;
  private static final String ENTITY_NAME = "testEntity";
  private static final String ASPECT_NAME = "testEntityProfile";
  private static final Urn TEST_URN = new TestEntityUrn("acryl", "testElasticSearchTimeseriesAspectService", "table1");
  private static final int NUM_ROWS = 100;
  private static final long TIME_INCREMENT = 3600000; // hour in ms.
  private static final String CONTENT_TYPE = "application/json";

  private ElasticsearchContainer _elasticsearchContainer;
  private RestHighLevelClient _searchClient;
  private EntityRegistry _entityRegistry;
  private IndexConvention _indexConvention;
  private ElasticSearchTimeseriesAspectService _elasticSearchTimeseriesAspectService;
  private AspectSpec _aspectSpec;

  private Map<Long, TestEntityProfile> _testEntityProfiles;
  private Long _startTime;

  @BeforeTest
  public void setup() {
    _entityRegistry = new ConfigEntityRegistry(new DataSchemaFactory("com.datahub.test"),
        TestEntityProfile.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
    _indexConvention = new IndexConventionImpl(null);
    _elasticsearchContainer = new ElasticsearchContainer(IMAGE_NAME);
    _elasticsearchContainer.start();
    _searchClient = buildRestClient();
    _elasticSearchTimeseriesAspectService = buildService();
    _elasticSearchTimeseriesAspectService.configure();
    EntitySpec entitySpec = _entityRegistry.getEntitySpec(ENTITY_NAME);
    _aspectSpec = entitySpec.getAspectSpec(ASPECT_NAME);
  }

  private void upsertDocument(TestEntityProfile dp) throws JsonProcessingException {
    _elasticSearchTimeseriesAspectService.upsertDocument(ENTITY_NAME, ASPECT_NAME,
        TimeseriesAspectTransformer.transform(TEST_URN, dp, null));
  }

  @Nonnull
  private RestHighLevelClient buildRestClient() {
    final RestClientBuilder builder =
        RestClient.builder(new HttpHost("localhost", _elasticsearchContainer.getMappedPort(HTTP_PORT), "http"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultIOReactorConfig(
                IOReactorConfig.custom().setIoThreadCount(1).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(3000));

    return new RestHighLevelClient(builder);
  }

  @Nonnull
  private ElasticSearchTimeseriesAspectService buildService() {
    return new ElasticSearchTimeseriesAspectService(_searchClient, _indexConvention,
        new TimeseriesAspectIndexBuilders(_entityRegistry, _searchClient, _indexConvention), 1, 1, 3, 1);
  }

  @AfterTest
  public void tearDown() {
    _elasticsearchContainer.stop();
  }

  private TestEntityProfile makeTestProfile(long eventTime, long stat) {
    TestEntityProfile testEntityProfile = new TestEntityProfile();
    testEntityProfile.setTimestampMillis(eventTime);
    testEntityProfile.setStat(stat);
    return testEntityProfile;
  }

  private void validateAspectValue(EnvelopedAspect envelopedAspectResult) {
    TestEntityProfile actualProfile =
        (TestEntityProfile) GenericAspectUtils.deserializeAspect(envelopedAspectResult.getAspect().getValue(),
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

  @Test(groups = "upsert")
  public void testUpsertProfiles() throws Exception {
    // Create the testEntity profiles that we would like to use for testing.
    _startTime = Calendar.getInstance().getTimeInMillis();

    TestEntityProfile firstProfile = makeTestProfile(_startTime, 20);
    Stream<TestEntityProfile> testEntityProfileStream = Stream.iterate(firstProfile,
        (TestEntityProfile prev) -> makeTestProfile(prev.getTimestampMillis() + TIME_INCREMENT, prev.getStat() + 10));

    _testEntityProfiles = testEntityProfileStream.limit(NUM_ROWS)
        .collect(Collectors.toMap(TestEntityProfile::getTimestampMillis, Function.identity()));
    Long endTime = _startTime + (NUM_ROWS - 1) * TIME_INCREMENT;

    assertNotNull(_testEntityProfiles.get(_startTime));
    assertNotNull(_testEntityProfiles.get(endTime));

    // Upsert the documents into the index.
    _testEntityProfiles.values().forEach(x -> {
      try {
        upsertDocument(x);
      } catch (JsonProcessingException jsonProcessingException) {
        jsonProcessingException.printStackTrace();
      }
    });

    syncAfterWrite(_searchClient);
  }

  @Test(groups = "query", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesAll() {
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME, null, null, NUM_ROWS);
    validateAspectValues(resultAspects, NUM_ROWS);
  }

  @Test(groups = "query", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesSubRangeInclusiveOverlap() {
    int expectedNumRows = 10;
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME, _startTime,
            _startTime + TIME_INCREMENT * (expectedNumRows - 1), expectedNumRows);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = "query", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesSubRangeExclusiveOverlap() {
    int expectedNumRows = 10;
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME,
            _startTime + TIME_INCREMENT / 2, _startTime + TIME_INCREMENT * expectedNumRows + TIME_INCREMENT / 2,
            expectedNumRows);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = "query", dependsOnGroups = "upsert")
  public void testGetAspectTimeseriesValuesExactlyOneResponse() {
    int expectedNumRows = 1;
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(TEST_URN, ENTITY_NAME, ASPECT_NAME,
            _startTime + TIME_INCREMENT / 2, _startTime + TIME_INCREMENT * 3 / 2, expectedNumRows);
    validateAspectValues(resultAspects, expectedNumRows);
  }

  @Test(groups = {"query"}, dependsOnGroups = {"upsert"})
  public void testGetAspectTimeseriesValueMissingUrn() {
    Urn nonExistingUrn = new TestEntityUrn("missing", "missing", "missing");
    List<EnvelopedAspect> resultAspects =
        _elasticSearchTimeseriesAspectService.getAspectValues(nonExistingUrn, ENTITY_NAME, ASPECT_NAME, null, null,
            NUM_ROWS);
    validateAspectValues(resultAspects, 0);
  }
}
