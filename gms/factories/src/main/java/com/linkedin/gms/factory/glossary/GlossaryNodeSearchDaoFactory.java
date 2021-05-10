package com.linkedin.gms.factory.glossary;

import com.linkedin.metadata.configs.GlossaryNodeSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.GlossaryNodeInfoDocument;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class GlossaryNodeSearchDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "glossaryNodeSearchDAO")
  @DependsOn({"elasticSearchRestHighLevelClient"})
  @Nonnull
  protected ESSearchDAO createInstance() {
    return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), GlossaryNodeInfoDocument.class,
        new GlossaryNodeSearchConfig());
  }
}