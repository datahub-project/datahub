package com.linkedin.gms.factory.dataset;

import com.linkedin.metadata.configs.BrowseConfigFactory;
import com.linkedin.metadata.dao.browse.ESBrowseDAO;
import com.linkedin.metadata.search.DatasetDocument;
import javax.annotation.Nonnull;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class DatasetBrowseDAOFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Nonnull
  @Bean(name = "datasetBrowseDao")
  @DependsOn({"elasticSearchRestHighLevelClient"})
  protected ESBrowseDAO createInstance() {
    return new ESBrowseDAO(applicationContext.getBean(RestHighLevelClient.class),
        BrowseConfigFactory.getBrowseConfig(DatasetDocument.class));
  }
}
