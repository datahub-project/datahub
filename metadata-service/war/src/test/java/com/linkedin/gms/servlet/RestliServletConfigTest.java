package com.linkedin.gms.servlet;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import org.mockito.Mockito;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.Test;

public class RestliServletConfigTest {

  /** Stands in for ElasticSearchServiceFactory, the one owner of elasticSearchConfiguration. */
  @Configuration
  static class SearchConfigOwner {
    @Bean
    public ElasticSearchConfiguration elasticSearchConfiguration() {
      return new ElasticSearchConfiguration();
    }

    @Bean
    public ConfigurationProvider configurationProvider() {
      return Mockito.mock(ConfigurationProvider.class);
    }
  }

  @Test
  public void testDoesNotRedefineElasticSearchConfiguration() {
    try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
      // A second elasticSearchConfiguration definition fails registration instead of silently
      // replacing the first one.
      context.setAllowBeanDefinitionOverriding(false);
      context.register(SearchConfigOwner.class, RestliServletConfig.class);
      // Only the two configurations under test are instantiated; the Rest.li server components
      // pulled in by the component scan stay lazy.
      context.addBeanFactoryPostProcessor(
          beanFactory -> {
            for (String name : beanFactory.getBeanDefinitionNames()) {
              beanFactory.getBeanDefinition(name).setLazyInit(true);
            }
          });
      context.refresh();

      assertNotNull(context.getBean(RestliServletConfig.class));
      String[] names = context.getBeanNamesForType(ElasticSearchConfiguration.class);
      assertEquals(names, new String[] {"elasticSearchConfiguration"});
      assertEquals(
          context.getBeanFactory().getBeanDefinition(names[0]).getFactoryBeanName(),
          "restliServletConfigTest.SearchConfigOwner");
    }
  }
}
