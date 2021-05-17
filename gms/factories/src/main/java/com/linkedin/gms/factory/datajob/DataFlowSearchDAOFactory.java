package com.linkedin.gms.factory.datajob;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.configs.DataFlowSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.DataFlowDocument;

import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;


@Configuration
public class DataFlowSearchDAOFactory {

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "dataFlowSearchDAO")
  @DependsOn({"elasticSearchRestHighLevelClient", IndexConventionFactory.INDEX_CONVENTION_BEAN})
  @Nonnull
  protected ESSearchDAO createInstance() {
    return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), DataFlowDocument.class,
        new DataFlowSearchConfig(applicationContext.getBean(IndexConvention.class)));
  }
}
