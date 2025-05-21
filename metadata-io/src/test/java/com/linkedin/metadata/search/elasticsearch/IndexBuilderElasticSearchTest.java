package com.linkedin.metadata.search.elasticsearch;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.indexbuilder.IndexBuilderTestBase;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.util.Map;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({ElasticSearchSuite.class, SearchTestContainerConfiguration.class})
public class IndexBuilderElasticSearchTest extends IndexBuilderTestBase {

  @Autowired private RestHighLevelClient _searchClient;

  @NotNull
  @Override
  protected RestHighLevelClient getSearchClient() {
    return _searchClient;
  }

  @Test
  public void initTest() {
    assertNotNull(_searchClient);
  }

  @Test
  public void testCodec() throws Exception {
    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    ESIndexBuilder customIndexBuilder =
        new ESIndexBuilder(
            getSearchClient(),
            1,
            0,
            1,
            0,
            Map.of(),
            false,
            false,
            false,
            new ElasticSearchConfiguration(),
            gitVersion);
    customIndexBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    GetIndexResponse resp = getTestIndex();
    assertEquals("default", resp.getSetting(TEST_INDEX_NAME, "index.codec"));
  }
}
