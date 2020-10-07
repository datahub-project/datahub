package com.linkedin.gms.factory.dataset;

import com.linkedin.metadata.configs.DatasetSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.DatasetDocument;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;


@Configuration
public class DatasetSearchDAOFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Nonnull
  @DependsOn({"elasticSearchRestHighLevelClient"})
  @Bean(name = "datasetSearchDao")
  protected ESSearchDAO createInstance() {
    return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), DatasetDocument.class,
        new DatasetSearchConfig());
  }
}