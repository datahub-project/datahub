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
  public void testShippedSpacedQueryFindsUnderscoreRealNameFirst() throws Exception {
    assertSpacedQueryFindsUnderscoreRealNameFirst(SHIPPED);
  }

  @Test
  public void testClassOrderSpacedQueryFindsUnderscoreRealNameFirst() throws Exception {
    assertSpacedQueryFindsUnderscoreRealNameFirst(CLASS_ORDER);
  }

  @Test
  public void testShippedThreeWordRealNameAboveOneEqualCopy() throws Exception {
    assertThreeWordRealNameAboveOneEqualCopy(SHIPPED);
  }

  @Test
  public void testClassOrderThreeWordRealNameAboveOneEqualCopy() throws Exception {
    assertThreeWordRealNameAboveOneEqualCopy(CLASS_ORDER);
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
}
