package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.query.KeywordSearchV3TestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

@Import({
  ElasticSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class KeywordSearchV3ElasticSearchTest extends KeywordSearchV3TestBase {
  @Getter @Autowired private SearchClientShim<?> searchClient;
  @Getter @Autowired private ESBulkProcessor bulkProcessor;
}
