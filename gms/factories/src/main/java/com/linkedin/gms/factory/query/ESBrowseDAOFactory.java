package com.linkedin.gms.factory.query;

import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class ESBrowseDAOFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "esBrowseDao")
  @DependsOn({"elasticSearchRestHighLevelClient", IndexConventionFactory.INDEX_CONVENTION_BEAN})
  @Nonnull
  protected ESBrowseDAO createInstance() {
    final EntityRegistry registry = SnapshotEntityRegistry.getInstance();

    return new ESBrowseDAO(
        registry,
        applicationContext.getBean(RestHighLevelClient.class),
        applicationContext.getBean(IndexConvention.class));
  }
}