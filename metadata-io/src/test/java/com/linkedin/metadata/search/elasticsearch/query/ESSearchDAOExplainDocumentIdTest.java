package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.digest.DigestUtils;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ESSearchDAOExplainDocumentIdTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");

  private static ESSearchDAO dao(ElasticSearchConfiguration config) {
    return new ESSearchDAO(
        Mockito.mock(SearchClientShim.class),
        false,
        config,
        null,
        QueryFilterRewriteChain.EMPTY,
        TEST_SEARCH_SERVICE_CONFIG);
  }

  private static ElasticSearchConfiguration v3KeywordReadConfig() {
    return TEST_OS_SEARCH_CONFIG.toBuilder()
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
                .v3(
                    EntityIndexVersionConfiguration.builder()
                        .enabled(true)
                        .keywordReadEnabled(true)
                        .build())
                .build())
        .build();
  }

  @Test
  public void testV2LeavesDocumentIdUnchanged() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    String encoded = URLEncoder.encode(DATASET_URN.toString(), StandardCharsets.UTF_8);
    ESSearchDAO searchDao = dao(TEST_OS_SEARCH_CONFIG);
    assertEquals(searchDao.documentIdForExplain(opContext, encoded), encoded);
    assertEquals(
        searchDao.documentIdForExplain(opContext, DATASET_URN.toString()), DATASET_URN.toString());
  }

  @Test
  public void testV3HashesUrnAndUrlEncodedUrn() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    String expected = DigestUtils.sha256Hex(DATASET_URN.toString());
    String encoded = URLEncoder.encode(DATASET_URN.toString(), StandardCharsets.UTF_8);
    ESSearchDAO searchDao = dao(v3KeywordReadConfig());

    assertEquals(searchDao.documentIdForExplain(opContext, DATASET_URN.toString()), expected);
    assertEquals(searchDao.documentIdForExplain(opContext, encoded), expected);
    assertEquals(searchDao.documentIdForExplain(opContext, expected), expected);
    assertNotEquals(encoded, expected);
  }
}
