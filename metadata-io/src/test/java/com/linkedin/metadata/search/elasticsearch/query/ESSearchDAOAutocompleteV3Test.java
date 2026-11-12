package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ESSearchDAOAutocompleteV3Test {
  private OperationContext opContext;
  private ESSearchDAO dao;

  @BeforeMethod
  public void setup() {
    opContext =
        TestOperationContexts.withFixedSearchClient(
            TestOperationContexts.systemContextNoSearchAuthorization(),
            mock(SearchClientShim.class));
    dao =
        new ESSearchDAO(
            false,
            TEST_OS_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            false,
            TEST_SEARCH_SERVICE_CONFIG,
            new Sha256UrnEntityDocumentIdHasher(),
            mock(HybridSearchResultReranker.class));
  }

  @Test
  public void testDefaultNameFieldKeepsWholeNamePrefix() {
    String omitted = prefixQuery(null);
    String explicitName = prefixQuery("name");

    assertTrue(omitted.contains("name.lowercase"), omitted);
    assertTrue(explicitName.contains("name.lowercase"), explicitName);
  }

  @Test
  public void testCustomFieldKeepsAnalyzedPrefixOnly() {
    String query = prefixQuery("description");

    assertFalse(query.contains("name.lowercase"), query);
  }

  private String prefixQuery(String field) {
    return dao.buildV3AutocompleteRequest(
            opContext, Constants.DATASET_ENTITY_NAME, "orders", field, null, 10)
        .source()
        .query()
        .toString();
  }
}
