package com.linkedin.gms.factory.glossary;

import com.linkedin.metadata.configs.GlossaryTermSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.GlossaryTermInfoDocument;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class GlossaryTermSearchDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "glossaryTermSearchDAO")
  @DependsOn({"elasticSearchRestHighLevelClient"})
  @Nonnull
  protected ESSearchDAO createInstance() {
    return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), GlossaryTermInfoDocument.class,
        new GlossaryTermSearchConfig());
  }
}