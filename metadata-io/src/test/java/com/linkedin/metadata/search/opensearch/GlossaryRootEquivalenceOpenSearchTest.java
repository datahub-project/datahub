package com.linkedin.metadata.search.opensearch;

import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.fixtures.GlossaryRootEquivalenceTestBase;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;

@Getter
@Import({
  OpenSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class GlossaryRootEquivalenceOpenSearchTest extends GlossaryRootEquivalenceTestBase {

  @Nonnull @Autowired private SearchClientShim<?> searchClient;
  @Nonnull @Autowired private ESBulkProcessor bulkProcessor;
  @Nonnull @Autowired private ESIndexBuilder indexBuilder;

  @Nonnull
  @Autowired
  @Qualifier("fixtureCustomSearchConfig")
  private CustomSearchConfiguration customSearchConfiguration;
}
