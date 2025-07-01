package com.linkedin.metadata.search.opensearch;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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

@Import({OpenSearchSuite.class, SearchTestContainerConfiguration.class})
public class IndexBuilderOpenSearchTest extends IndexBuilderTestBase {

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
            TEST_ES_SEARCH_CONFIG,
            gitVersion);
    customIndexBuilder.buildIndex(TEST_INDEX_NAME, Map.of(), Map.of());
    GetIndexResponse resp = getTestIndex();
    assertEquals("zstd_no_dict", resp.getSetting(TEST_INDEX_NAME, "index.codec"));
  }
}
