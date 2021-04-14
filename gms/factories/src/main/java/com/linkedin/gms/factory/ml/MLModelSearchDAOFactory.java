package com.linkedin.gms.factory.ml;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.configs.MLModelSearchConfig;
import com.linkedin.metadata.dao.search.ESSearchDAO;
import com.linkedin.metadata.search.MLModelDocument;

import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;


@Configuration
public class MLModelSearchDAOFactory {

  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "mlModelSearchDAO")
  @DependsOn({"elasticSearchRestHighLevelClient", IndexConventionFactory.INDEX_CONVENTION_BEAN})
  @Nonnull
  protected ESSearchDAO createInstance() {
    return new ESSearchDAO(applicationContext.getBean(RestHighLevelClient.class), MLModelDocument.class,
        new MLModelSearchConfig(applicationContext.getBean(IndexConvention.class)));
  }
}
