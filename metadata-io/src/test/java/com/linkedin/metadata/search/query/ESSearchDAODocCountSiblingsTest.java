package com.linkedin.metadata.search.query;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

/** Entity doc counts must hide non-primary siblings exactly when search does. */
public class ESSearchDAODocCountSiblingsTest {

  private final OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  private static String docCountQuery(OperationContext opContext, boolean hideNonPrimary) {
    ElasticSearchConfiguration config =
        TEST_OS_SEARCH_CONFIG.toBuilder()
            .search(
                TEST_OS_SEARCH_CONFIG.getSearch().toBuilder()
                    .hideNonPrimarySiblings(hideNonPrimary)
                    .build())
            .build();
    ESSearchDAO dao =
        new ESSearchDAO(
            false, config, null, QueryFilterRewriteChain.EMPTY, TEST_SEARCH_SERVICE_CONFIG);
    return dao.buildDocCountRequest(opContext, DATASET_ENTITY_NAME, null).query().toString();
  }

  @Test
  public void testDocCountHidesNonPrimarySiblingsWhenEnabled() {
    String query = docCountQuery(opContext, true);
    assertTrue(query.contains("siblingsPrimary"), query);
  }

  @Test
  public void testDocCountUnchangedWhenDisabled() {
    String query = docCountQuery(opContext, false);
    assertFalse(query.contains("siblingsPrimary"), query);
  }
}
