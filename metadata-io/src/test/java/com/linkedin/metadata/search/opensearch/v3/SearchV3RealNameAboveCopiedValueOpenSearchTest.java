package com.linkedin.metadata.search.opensearch.v3;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.opensearch.OpenSearchSuite;
import com.linkedin.metadata.search.quality.SearchV3RealNameAboveCopiedValueTestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

@Import({
  OpenSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class SearchV3RealNameAboveCopiedValueOpenSearchTest
    extends SearchV3RealNameAboveCopiedValueTestBase {
  private static final String SHIPPED = null;

  @Getter @Autowired private SearchClientShim<?> searchClient;
  @Getter @Autowired private ESBulkProcessor bulkProcessor;

  @Test
  public void testShippedUnquotedQueryExactOnlyForWholeName() throws Exception {
    assertUnquotedQueryExactOnlyForWholeName(SHIPPED);
  }

  @Test
  public void testClassOrderUnquotedQueryExactOnlyForWholeName() throws Exception {
    assertUnquotedQueryExactOnlyForWholeName(CLASS_ORDER);
  }

  @Test
  public void testShippedQuotedQueryExactOnlyForWholeName() throws Exception {
    assertQuotedQueryExactOnlyForWholeName(SHIPPED);
  }

  @Test
  public void testClassOrderQuotedQueryExactOnlyForWholeName() throws Exception {
    assertQuotedQueryExactOnlyForWholeName(CLASS_ORDER);
  }

  @Test
  public void testShippedExactCopiedOnlyForWholeTitle() throws Exception {
    assertExactCopiedOnlyForWholeTitle(SHIPPED);
  }

  @Test
  public void testClassOrderExactCopiedOnlyForWholeTitle() throws Exception {
    assertExactCopiedOnlyForWholeTitle(CLASS_ORDER);
  }

  @Test
  public void testShippedStemOnlyNameBelowExactCopiedTitle() throws Exception {
    assertStemOnlyNameBelowExactCopiedTitle(SHIPPED);
  }

  @Test
  public void testClassOrderStemOnlyNameBelowExactCopiedTitle() throws Exception {
    assertStemOnlyNameBelowExactCopiedTitle(CLASS_ORDER);
  }

  @Test
  public void testShippedRealNameEqualToOwnTitleIsExactRealName() throws Exception {
    assertRealNameEqualToOwnTitleIsExactRealName(SHIPPED);
  }

  @Test
  public void testClassOrderRealNameEqualToOwnTitleIsExactRealName() throws Exception {
    assertRealNameEqualToOwnTitleIsExactRealName(CLASS_ORDER);
  }

  @Test
  public void testShippedPartialRealNameWithOwnTitleRanksAsRealName() throws Exception {
    assertPartialRealNameWithOwnTitleRanksAsRealName(SHIPPED);
  }

  @Test
  public void testClassOrderPartialRealNameWithOwnTitleRanksAsRealName() throws Exception {
    assertPartialRealNameWithOwnTitleRanksAsRealName(CLASS_ORDER);
  }

  @Test
  public void testShippedCanonicalExampleOrder() throws Exception {
    assertCanonicalExampleOrder(SHIPPED);
  }

  @Test
  public void testClassOrderCanonicalExampleOrder() throws Exception {
    assertCanonicalExampleOrder(CLASS_ORDER);
  }
}
