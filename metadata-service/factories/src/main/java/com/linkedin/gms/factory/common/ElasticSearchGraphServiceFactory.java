package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.graph.LineageRegistry;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({BaseElasticSearchComponentsFactory.class, EntityRegistryFactory.class})
public class ElasticSearchGraphServiceFactory {
  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Autowired
  private CacheManager cacheManager;

  @Bean(name = "elasticSearchGraphService")
  @Nonnull
  protected ElasticSearchGraphService getInstance() {
    return new ElasticSearchGraphService(components.getSearchClient(), components.getIndexConvention(),
        new ESGraphWriteDAO(components.getSearchClient(), components.getIndexConvention(),
            components.getBulkProcessor()),
        new ESGraphQueryDAO(components.getSearchClient(), new LineageRegistry(entityRegistry),
            components.getIndexConvention(), cacheManager.getCache("elasticSearchGraphService")),
        components.getIndexBuilder());
  }
}
