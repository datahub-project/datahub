package com.linkedin.metadata;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import io.datahub.test.fixtures.elasticsearch.EntityExporter;
import io.datahub.test.fixtures.elasticsearch.FixtureReader;
import io.datahub.test.fixtures.elasticsearch.FixtureWriter;
import io.datahub.test.fixtures.elasticsearch.LineageExporter;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Set;

import static com.linkedin.metadata.ESTestUtils.environmentRestClientBuilder;

@TestConfiguration
@Import(ESTestConfiguration.class)
public class ESTestFixtureUtils {

    @Test
    @Ignore("Fixture capture lineage")
    /*
     * Run this to capture test fixtures
     * 1. Update environment variables for ELASTICSEARCH_* (see buildEnvironmentClient)
     * 2. Update fixture name
     * 3. Comment @Ignore
     * 4. Run extraction
     **/
    private void extractSearchLineageTestFixture() throws IOException {
        String rootUrn = "urn:li:dataset:(urn:li:dataPlatform:teradata,teradata.simba.pp_access_views.dim_cust,PROD)";

        // Set.of("system_metadata_service_v1", "datasetindex_v2", "graph_service_v1")
        try (RestHighLevelClient client = new RestHighLevelClient(environmentRestClientBuilder())) {
            FixtureWriter fixtureWriter = FixtureWriter.builder()
                    .client(client)
                    .build();

            LineageExporter exporter = LineageExporter.builder()
                    .writer(fixtureWriter)
                    .graphIndexName("<namespace>_graph_service_v1-5shards")
                    .graphOutputPath(String.format("%s/%s.json", "search_lineage", "graph_service_v1"))
                    //.entityIndexName("<namespace>_datasetindex_v2-5shards")
                    //.entityOutputPath(String.format("%s/%s.json", "search_lineage", "datasetindex_v2"))
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
        String fixtureName = "long_tail";
        String prefix = "";
        String commonSuffix = "index_v2";

        try (RestHighLevelClient client = new RestHighLevelClient(environmentRestClientBuilder())) {
            FixtureWriter fixtureWriter = FixtureWriter.builder()
                    .client(client)
                    .build();

            EntityExporter exporter = EntityExporter.builder()
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
        ESBulkProcessor bulkProcessor = ESBulkProcessor.builder(new RestHighLevelClient(environmentRestClientBuilder()))
                .async(true)
                .bulkRequestsLimit(1000)
                .retryInterval(1L)
                .numRetries(2)
                .build();

        FixtureReader reader = FixtureReader.builder()
                .bulkProcessor(bulkProcessor)
                .fixtureName("long_tail")
                .build();

        reader.read();
    }
}
