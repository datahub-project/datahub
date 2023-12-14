package io.datahubproject.test.fixtures.search;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import io.datahubproject.test.models.DatasetAnonymized;
import io.datahubproject.test.search.ElasticsearchTestContainer;
import io.datahubproject.test.search.SearchTestUtils;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.io.IOException;
import java.util.Set;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

/** This class is used for extracting and moving search fixture data. */
@TestConfiguration
public class SearchFixtureUtils {

  public static final String FIXTURE_BASE = "src/test/resources/elasticsearch";

  public static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  static {
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    OBJECT_MAPPER
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
  }

  @Bean(name = "testSearchContainer")
  public GenericContainer<?> testSearchContainer() {
    return new ElasticsearchTestContainer().startContainer();
  }

  @Test
  @Ignore("Fixture capture lineage")
  /*
   * Run this to capture test fixtures, repeat for graph & dataset
   * 1. Configure anonymizer class (use builder or set to null) Do not commit non-anonymous data
   * 2. Update environment variables for ELASTICSEARCH_* (see buildEnvironmentClient)
   * 2. Update fixture name
   * 3. Comment @Ignore
   * 4. Create output directory
   * 5. Run extraction
   **/
  private void extractSearchLineageTestFixture() throws IOException {
    String rootUrn =
        "urn:li:dataset:(urn:li:dataPlatform:teradata,teradata.simba.pp_bi_tables.tmis_daily_metrics_final_agg,PROD)";

    // Set.of("system_metadata_service_v1", "datasetindex_v2", "graph_service_v1")
    try (RestHighLevelClient client =
        new RestHighLevelClient(SearchTestUtils.environmentRestClientBuilder())) {
      FixtureWriter fixtureWriter = FixtureWriter.builder().client(client).build();

      /*
         LineageExporter<GraphAnonymized> exporter = LineageExporter.<GraphAnonymized>builder()
                 .writer(fixtureWriter)
                 .anonymizerClazz(GraphAnonymized.class)
                 .graphIndexName("<namespace>_graph_service_v1-5shards")
                 .graphOutputPath(String.format("%s/%s.json", "search_lineage2", "graph_service_v1"))
                 .build();
      */

      LineageExporter<DatasetAnonymized> exporter =
          LineageExporter.<DatasetAnonymized>builder()
              .writer(fixtureWriter)
              .anonymizerClazz(DatasetAnonymized.class)
              .entityIndexName("<namespace>_datasetindex_v2-5shards")
              .entityOutputPath(String.format("%s/%s.json", "search_lineage2", "datasetindex_v2"))
              .build();

      exporter.export(Set.of(rootUrn));
    }
  }

  @Test
  @Ignore("Fixture capture logic")
  /*
   * Run this to capture test fixtures
   * 1. Update environment variables for ELASTICSEARCH_* (see buildEnvironmentClient)
   * 2. Update fixture name
   * 3. Comment @Ignore
   * 4. Run extraction
   **/
  private void extractEntityTestFixture() throws IOException {
    String fixtureName = "temp";
    String prefix = "";
    String commonSuffix = "index_v2";

    try (RestHighLevelClient client =
        new RestHighLevelClient(SearchTestUtils.environmentRestClientBuilder())) {
      FixtureWriter fixtureWriter = FixtureWriter.builder().client(client).build();

      EntityExporter exporter =
          EntityExporter.builder()
              .client(client)
              .writer(fixtureWriter)
              .fixtureName(fixtureName)
              .sourceIndexSuffix(commonSuffix)
              .sourceIndexPrefix(prefix)
              .build();

      exporter.export();
    }
  }

  @Test
  @Ignore("Write capture logic to some external ES cluster for testing")
  /*
   * Can be used to write fixture data to external ES cluster
   * 1. Set environment variables
   * 2. Update fixture name and prefix
   * 3. Uncomment and run test
   */
  private void reindexTestFixtureData() throws IOException {
    ESBulkProcessor bulkProcessor =
        ESBulkProcessor.builder(
                new RestHighLevelClient(SearchTestUtils.environmentRestClientBuilder()))
            .async(true)
            .bulkRequestsLimit(1000)
            .retryInterval(1L)
            .numRetries(2)
            .build();

    FixtureReader reader =
        FixtureReader.builder()
            .bulkProcessor(bulkProcessor)
            .fixtureName("long_tail")
            .refreshIntervalSeconds(SearchTestContainerConfiguration.REFRESH_INTERVAL_SECONDS)
            .build();

    reader.read();
  }
}
