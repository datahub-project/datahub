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
  @Getter @Autowired private SearchClientShim<?> searchClient;
  @Getter @Autowired private ESBulkProcessor bulkProcessor;

  @Test
  public void testShippedUnquotedQueryExactOnlyForWholeName() throws Exception {
    assertUnquotedQueryExactOnlyForWholeName(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestUnquotedQueryExactOnlyForWholeName() throws Exception {
    assertUnquotedQueryExactOnlyForWholeName(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultUnquotedQueryExactOnlyForWholeName() throws Exception {
    assertUnquotedQueryExactOnlyForWholeName(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedQuotedQueryExactOnlyForWholeName() throws Exception {
    assertQuotedQueryExactOnlyForWholeName(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestQuotedQueryExactOnlyForWholeName() throws Exception {
    assertQuotedQueryExactOnlyForWholeName(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultQuotedQueryExactOnlyForWholeName() throws Exception {
    assertQuotedQueryExactOnlyForWholeName(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedExactCopiedOnlyForWholeTitle() throws Exception {
    assertExactCopiedOnlyForWholeTitle(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestExactCopiedOnlyForWholeTitle() throws Exception {
    assertExactCopiedOnlyForWholeTitle(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultExactCopiedOnlyForWholeTitle() throws Exception {
    assertExactCopiedOnlyForWholeTitle(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedStemOnlyNameBelowExactCopiedTitle() throws Exception {
    assertStemOnlyNameBelowExactCopiedTitle(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestStemOnlyNameBelowExactCopiedTitle() throws Exception {
    assertStemOnlyNameBelowExactCopiedTitle(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultStemOnlyNameBelowExactCopiedTitle() throws Exception {
    assertStemOnlyNameBelowExactCopiedTitle(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedCanonicalExampleOrder() throws Exception {
    assertCanonicalExampleOrder(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestCanonicalExampleOrder() throws Exception {
    assertCanonicalExampleOrder(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultCanonicalExampleOrder() throws Exception {
    assertCanonicalExampleOrder(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedRealNameEqualToOwnTitleIsExactRealName() throws Exception {
    assertRealNameEqualToOwnTitleIsExactRealName(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestRealNameEqualToOwnTitleIsExactRealName() throws Exception {
    assertRealNameEqualToOwnTitleIsExactRealName(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultRealNameEqualToOwnTitleIsExactRealName() throws Exception {
    assertRealNameEqualToOwnTitleIsExactRealName(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedPartialRealNameWithOwnTitleRanksAsRealName() throws Exception {
    assertPartialRealNameWithOwnTitleRanksAsRealName(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestPartialRealNameWithOwnTitleRanksAsRealName()
      throws Exception {
    assertPartialRealNameWithOwnTitleRanksAsRealName(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultPartialRealNameWithOwnTitleRanksAsRealName() throws Exception {
    assertPartialRealNameWithOwnTitleRanksAsRealName(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedColumnsFollowDatasetsOfTheSameMatch() throws Exception {
    assertColumnsFollowDatasetsOfTheSameMatch(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestColumnsFollowDatasetsOfTheSameMatch() throws Exception {
    assertColumnsFollowDatasetsOfTheSameMatch(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultColumnsFollowDatasetsOfTheSameMatch() throws Exception {
    assertColumnsFollowDatasetsOfTheSameMatch(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedRecordIdFindsItsAssetFirst() throws Exception {
    assertRecordIdFindsItsAssetFirst(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestRecordIdFindsItsAssetFirst() throws Exception {
    assertRecordIdFindsItsAssetFirst(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultRecordIdFindsItsAssetFirst() throws Exception {
    assertRecordIdFindsItsAssetFirst(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedPagingKeepsTrancheOrder() throws Exception {
    assertPagingKeepsTrancheOrder(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestPagingKeepsTrancheOrder() throws Exception {
    assertPagingKeepsTrancheOrder(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultPagingKeepsTrancheOrder() throws Exception {
    assertPagingKeepsTrancheOrder(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testShippedExactNameWithLongerOwnTitleStaysFirst() throws Exception {
    assertExactNameWithLongerOwnTitleStaysFirst(Mode.SHIPPED);
  }

  @Test
  public void testClassOrderPerRequestExactNameWithLongerOwnTitleStaysFirst() throws Exception {
    assertExactNameWithLongerOwnTitleStaysFirst(Mode.CLASS_ORDER_PER_REQUEST);
  }

  @Test
  public void testClassOrderDefaultExactNameWithLongerOwnTitleStaysFirst() throws Exception {
    assertExactNameWithLongerOwnTitleStaysFirst(Mode.CLASS_ORDER_DEFAULT);
  }
}
