package com.linkedin.gms.factory.identity;

import com.linkedin.metadata.configs.CorpGroupSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.CorpGroupDocument;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class CorpGroupSearchDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "corpGroupSearchDAO")
  @DependsOn({"elasticSearchRestHighLevelClient"})
  @Nonnull
  protected ESSearchDAO createInstance() {
    return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), CorpGroupDocument.class,
        new CorpGroupSearchConfig());
  }
}