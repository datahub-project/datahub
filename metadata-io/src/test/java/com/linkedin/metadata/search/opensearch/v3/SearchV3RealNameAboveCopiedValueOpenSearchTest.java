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

/** The ranking scenarios with the class-order router as the configured search-box default. */
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
  public void testClassOrderDefaultUnquotedQueryExactOnlyForWholeName() throws Exception {
    assertUnquotedQueryExactOnlyForWholeName();
  }

  @Test
  public void testClassOrderDefaultExactCopiedOnlyForWholeTitle() throws Exception {
    assertExactCopiedOnlyForWholeTitle();
  }

  @Test
  public void testClassOrderDefaultStemOnlyNameBelowExactCopiedTitle() throws Exception {
    assertStemOnlyNameBelowExactCopiedTitle();
  }

  @Test
  public void testClassOrderDefaultCanonicalExampleOrder() throws Exception {
    assertCanonicalExampleOrder();
  }

  @Test
  public void testClassOrderDefaultRealNameEqualToOwnTitleIsExactRealName() throws Exception {
    assertRealNameEqualToOwnTitleIsExactRealName();
  }

  @Test
  public void testClassOrderDefaultPartialRealNameWithOwnTitleRanksAsRealName() throws Exception {
    assertPartialRealNameWithOwnTitleRanksAsRealName();
  }

  @Test
  public void testClassOrderDefaultColumnsFollowDatasetsOfTheSameMatch() throws Exception {
    assertColumnsFollowDatasetsOfTheSameMatch();
  }

  @Test
  public void testClassOrderDefaultRecordIdFindsItsAssetFirst() throws Exception {
    assertRecordIdFindsItsAssetFirst();
  }

  @Test
  public void testClassOrderDefaultPagingKeepsTrancheOrder() throws Exception {
    assertPagingKeepsTrancheOrder();
  }

  @Test
  public void testClassOrderDefaultExactNameRanksFirstAboveLongerMatches() throws Exception {
    assertExactNameRanksFirstAboveLongerMatches();
  }

  @Test
  public void testClassOrderDefaultExactNameWithoutOwnTitleRanksFirst() throws Exception {
    assertExactNameWithoutOwnTitleRanksFirst();
  }

  @Test
  public void testClassOrderDefaultExactTitleRanksAboveLongerMatches() throws Exception {
    assertExactTitleRanksAboveLongerMatches();
  }
}
