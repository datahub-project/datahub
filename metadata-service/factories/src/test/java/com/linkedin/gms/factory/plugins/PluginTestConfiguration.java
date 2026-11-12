package com.linkedin.gms.factory.plugins;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import org.mockito.Answers;
import org.mockito.Mockito;

/**
 * A {@link ConfigurationProvider} for plugin wiring tests. Nested search objects stay mocks, and
 * the template limits match application.yaml. Java defaults are 0, which the template loader
 * rejects when the context starts.
 */
final class PluginTestConfiguration {

  private PluginTestConfiguration() {}

  static ConfigurationProvider configurationProvider() {
    ConfigurationProvider provider =
        Mockito.mock(ConfigurationProvider.class, Answers.RETURNS_MOCKS);
    ElasticSearchConfiguration elastic =
        Mockito.mock(ElasticSearchConfiguration.class, Answers.RETURNS_MOCKS);
    SearchConfiguration search = Mockito.mock(SearchConfiguration.class, Answers.RETURNS_MOCKS);
    SearchTemplateConfiguration templates = new SearchTemplateConfiguration();
    templates.setHybridKeywordScale(500);
    templates.setMaxParentDepth(5);
    templates.setCacheMaximumSize(500);
    templates.setMaxLeafClauses(1024);
    templates.setMaxRoutes(32);
    templates.setMaxRoutePatternLength(256);
    templates.setMaxRouteHops(5);
    templates.setMaxRankingSignals(32);
    templates.setMaxRankingWindow(1000);
    SearchV3Configuration v3 = new SearchV3Configuration().setTemplates(templates);
    Mockito.when(search.getV3()).thenReturn(v3);
    Mockito.when(elastic.getSearch()).thenReturn(search);
    Mockito.when(provider.getElasticSearch()).thenReturn(elastic);
    return provider;
  }
}
