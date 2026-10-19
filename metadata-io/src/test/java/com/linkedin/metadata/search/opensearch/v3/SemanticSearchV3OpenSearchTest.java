package com.linkedin.metadata.search.opensearch.v3;

import com.linkedin.metadata.search.opensearch.OpenSearchSuite;
import com.linkedin.metadata.search.semantic.v3.SemanticSearchV3TestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

@Import({
  OpenSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class SemanticSearchV3OpenSearchTest extends SemanticSearchV3TestBase {
  @Getter @Autowired private SearchClientShim<?> searchClient;
}
