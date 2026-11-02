package com.linkedin.metadata.search.elasticsearch.v3;

import com.linkedin.metadata.search.elasticsearch.ElasticSearchSuite;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.quality.SearchV3RealNameAboveCopiedValueTestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.testng.annotations.Test;

/** The ranking scenarios with the class-order router as the configured search-box default. */
@Import({
  ElasticSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class SearchV3RealNameAboveCopiedValueElasticSearchTest
    extends SearchV3RealNameAboveCopiedValueTestBase {
  @Getter @Autowired private SearchClientShim<?> searchClient;
  @Getter @Autowired private ESBulkProcessor bulkProcessor;

  @Test
  public void testClassOrderDefaultUnquotedQueryExactOnlyForWholeName() throws Exception {
    assertUnquotedQueryExactOnlyForWholeName(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyUnquotedQueryExactOnlyForWholeName() throws Exception {
    assertUnquotedQueryExactOnlyForWholeName(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultExactCopiedOnlyForWholeTitle() throws Exception {
    assertExactCopiedOnlyForWholeTitle(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyExactCopiedOnlyForWholeTitle() throws Exception {
    assertExactCopiedOnlyForWholeTitle(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultCanonicalExampleOrder() throws Exception {
    assertCanonicalExampleOrder(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyCanonicalExampleOrder() throws Exception {
    assertCanonicalExampleOrder(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultRealNameEqualToOwnTitleIsExactRealName() throws Exception {
    assertRealNameEqualToOwnTitleIsExactRealName(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyRealNameEqualToOwnTitleIsExactRealName() throws Exception {
    assertRealNameEqualToOwnTitleIsExactRealName(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultStemOnlyNamesRankBelowEveryMatch() throws Exception {
    assertStemOnlyNamesRankBelowEveryMatch(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyStemOnlyNamesRankBelowEveryMatch() throws Exception {
    assertStemOnlyNamesRankBelowEveryMatch(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultColumnsFollowDatasetsOfTheSameMatch() throws Exception {
    assertColumnsFollowDatasetsOfTheSameMatch(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyColumnsFollowDatasetsOfTheSameMatch() throws Exception {
    assertColumnsFollowDatasetsOfTheSameMatch(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultRecordIdFindsItsAssetFirst() throws Exception {
    assertRecordIdFindsItsAssetFirst(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyRecordIdFindsItsAssetFirst() throws Exception {
    assertRecordIdFindsItsAssetFirst(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultPagingKeepsTrancheOrder() throws Exception {
    assertPagingKeepsTrancheOrder(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyPagingKeepsTrancheOrder() throws Exception {
    assertPagingKeepsTrancheOrder(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultExactNameRanksFirstAboveLongerMatches() throws Exception {
    assertExactNameRanksFirstAboveLongerMatches(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyExactNameRanksFirstAboveLongerMatches() throws Exception {
    assertExactNameRanksFirstAboveLongerMatches(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultExactTitleRanksAboveLongerMatches() throws Exception {
    assertExactTitleRanksAboveLongerMatches(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyExactTitleRanksAboveLongerMatches() throws Exception {
    assertExactTitleRanksAboveLongerMatches(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }

  @Test
  public void testClassOrderDefaultExactNameWithoutOwnTitleRanksFirst() throws Exception {
    assertExactNameWithoutOwnTitleRanksFirst(Mode.CLASS_ORDER_DEFAULT);
  }

  @Test
  public void testClassOrderDefaultNoCopyExactNameWithoutOwnTitleRanksFirst() throws Exception {
    assertExactNameWithoutOwnTitleRanksFirst(Mode.CLASS_ORDER_DEFAULT_NO_COPY);
  }
}
