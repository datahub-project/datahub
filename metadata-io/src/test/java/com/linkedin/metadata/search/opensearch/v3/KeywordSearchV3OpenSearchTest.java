package com.linkedin.metadata.search.opensearch.v3;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.opensearch.OpenSearchSuite;
import com.linkedin.metadata.search.query.v3.KeywordSearchV3TestBase;
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
public class KeywordSearchV3OpenSearchTest extends KeywordSearchV3TestBase {
  @Getter @Autowired private SearchClientShim<?> searchClient;
  @Getter @Autowired private ESBulkProcessor bulkProcessor;

  @Test
  public void testSpacelessDottedPathOutranksPartialOverlap() throws Exception {
    assertSpacelessDottedPathOutranksPartialOverlap();
  }
}
