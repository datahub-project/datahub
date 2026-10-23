package com.linkedin.metadata.search.opensearch.v3;

import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.opensearch.OpenSearchSuite;
import com.linkedin.metadata.search.quality.SearchQualityV3TestBase;
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
public class SearchQualityV3OpenSearchTest extends SearchQualityV3TestBase {
  @Getter @Autowired private SearchClientShim<?> searchClient;
  @Getter @Autowired private ESBulkProcessor bulkProcessor;

  @Override
  protected String engineName() {
    return "opensearch";
  }

  // Redeclared so Gradle's --tests filter sees them. It does not scan TestNG methods
  // inherited from a superclass.
  @Test
  @Override
  public void testEveryShardWasIndexed() {
    super.testEveryShardWasIndexed();
  }

  @Test
  @Override
  public void testRepresentativeEntityFromEachShardIsSearchable() throws Exception {
    super.testRepresentativeEntityFromEachShardIsSearchable();
  }

  @Test
  @Override
  public void testSearchEntitiesAndLineageRoutes() {
    super.testSearchEntitiesAndLineageRoutes();
  }

  @Test
  @Override
  public void testBenchmarkNdcgAndProbes() throws Exception {
    super.testBenchmarkNdcgAndProbes();
  }

  @Test
  @Override
  public void testJavaBuilderBenchmarkNdcg() throws Exception {
    super.testJavaBuilderBenchmarkNdcg();
  }

  @Test
  @Override
  public void testLongtailNdcg() throws Exception {
    super.testLongtailNdcg();
  }

  @Test
  @Override
  public void testJavaBuilderLongtailNdcg() throws Exception {
    super.testJavaBuilderLongtailNdcg();
  }
}
