package com.linkedin.metadata.search.opensearch;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.indexbuilder.IndexBuilderTestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.util.Map;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({OpenSearchSuite.class, SearchTestContainerConfiguration.class})
public class IndexBuilderOpenSearchTest extends IndexBuilderTestBase {

  @Autowired private SearchClientShim<?> _searchClient;

  @NotNull
  @Override
  protected SearchClientShim getSearchClient() {
    return _searchClient;
  }

  @NotNull
  @Override
  protected ElasticSearchConfiguration getElasticSearchConfiguration() {
    return TEST_OS_SEARCH_CONFIG;
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
            TEST_ES_SEARCH_CONFIG,
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);
    ReindexConfig reindexConfig =
        customIndexBuilder.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    customIndexBuilder.buildIndex(reindexConfig);
    GetIndexResponse resp = getTestIndex();
    assertEquals("zstd_no_dict", resp.getSetting(TEST_INDEX_NAME, "index.codec"));
  }
}
