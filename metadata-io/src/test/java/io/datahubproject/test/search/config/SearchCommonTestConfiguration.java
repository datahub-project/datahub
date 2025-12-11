/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.test.search.config;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.createDelegatingMappingsBuilder;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/** This is common configuration for search regardless of which test container implementation. */
@TestConfiguration
public class SearchCommonTestConfiguration {
  @Bean
  public SearchConfiguration searchConfiguration() {
    SearchConfiguration searchConfiguration = TEST_OS_SEARCH_CONFIG.getSearch();
    searchConfiguration.setMaxTermBucketSize(20);

    ExactMatchConfiguration exactMatchConfiguration = new ExactMatchConfiguration();
    exactMatchConfiguration.setExclusive(false);
    exactMatchConfiguration.setExactFactor(10.0f);
    exactMatchConfiguration.setWithPrefix(true);
    exactMatchConfiguration.setPrefixFactor(6.0f);
    exactMatchConfiguration.setCaseSensitivityFactor(0.7f);
    exactMatchConfiguration.setEnableStructured(true);

    WordGramConfiguration wordGramConfiguration = new WordGramConfiguration();
    wordGramConfiguration.setTwoGramFactor(1.2f);
    wordGramConfiguration.setThreeGramFactor(1.5f);
    wordGramConfiguration.setFourGramFactor(1.8f);

    PartialConfiguration partialConfiguration = new PartialConfiguration();
    partialConfiguration.setFactor(0.4f);
    partialConfiguration.setUrnFactor(0.5f);

    searchConfiguration.setExactMatch(exactMatchConfiguration);
    searchConfiguration.setWordGram(wordGramConfiguration);
    searchConfiguration.setPartial(partialConfiguration);
    return searchConfiguration;
  }

  @Bean("defaultTestCustomSearchConfig")
  public CustomSearchConfiguration defaultTestCustomSearchConfig() throws Exception {
    CustomConfiguration customConfiguration = new CustomConfiguration();
    customConfiguration.setEnabled(true);
    customConfiguration.setFile("search_config_builder_test.yml");
    return customConfiguration.resolve(new YAMLMapper());
  }

  @Bean("fixtureCustomSearchConfig")
  public CustomSearchConfiguration fixtureCustomSearchConfig() throws Exception {
    CustomConfiguration customConfiguration = new CustomConfiguration();
    customConfiguration.setEnabled(true);
    customConfiguration.setFile("search_config_fixture_test.yml");
    return customConfiguration.resolve(new YAMLMapper());
  }

  @Bean(name = "queryOperationContext")
  public OperationContext queryOperationContext() {
    OperationContext testOpContext = TestOperationContexts.systemContextNoSearchAuthorization();

    // Create IndexConvention for the SearchContext
    IndexConvention indexConvention =
        new IndexConventionImpl(
            IndexConventionImpl.IndexConventionConfig.builder()
                .prefix("test")
                .hashIdAlgo("MD5")
                .build(),
            TEST_OS_SEARCH_CONFIG.getEntityIndex());

    // Create real SearchContext using ESUtils methods to populate searchableFieldTypes
    MappingsBuilder mappingsBuilder =
        createDelegatingMappingsBuilder(TEST_OS_SEARCH_CONFIG.getEntityIndex());
    SearchContext searchContext =
        SearchContext.builder()
            .indexConvention(indexConvention)
            .searchableFieldTypes(
                ESUtils.buildSearchableFieldTypes(
                    testOpContext.getEntityRegistry(), mappingsBuilder))
            .searchableFieldPaths(
                ESUtils.buildSearchableFieldPaths(testOpContext.getEntityRegistry()))
            .build();

    return testOpContext.toBuilder()
        .searchContext(searchContext)
        .build(testOpContext.getSessionAuthentication(), true);
  }

  @Bean
  public QueryFilterRewriteChain queryFilterRewriteChain() {
    return QueryFilterRewriteChain.EMPTY;
  }
}
