package com.linkedin.gms.factory.plugins;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.SearchV3Configuration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import org.mockito.Answers;
import org.mockito.Mockito;

/**
 * A {@link ConfigurationProvider} for plugin wiring tests. Nested search objects stay mocks, and
 * the template limits match application.yaml. Java defaults are 0, which the template loader
 * rejects when the context starts. Semantic search is a real object: a mock answers {@code
 * defaultBinding} with {@code ""}, and the profile registry rejects that unknown name.
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
    EntityIndexConfiguration entityIndex =
        Mockito.mock(EntityIndexConfiguration.class, Answers.RETURNS_MOCKS);
    Mockito.when(entityIndex.getSemanticSearch()).thenReturn(new SemanticSearchConfiguration());
    Mockito.when(elastic.getEntityIndex()).thenReturn(entityIndex);
    Mockito.when(provider.getElasticSearch()).thenReturn(elastic);
    return provider;
  }
}
