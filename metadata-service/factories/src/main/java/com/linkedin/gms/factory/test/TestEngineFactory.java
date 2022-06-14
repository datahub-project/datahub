package com.linkedin.gms.factory.test;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.TestFetcher;
import com.linkedin.metadata.test.definition.TestDefinitionProvider;
import com.linkedin.metadata.test.eval.TestPredicateEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;


@Configuration
@Import({EntityRegistryFactory.class, EntityServiceFactory.class, EntitySearchServiceFactory.class})
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class TestEngineFactory {
  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Autowired
  @Qualifier("entityService")
  private EntityService entityService;

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Autowired
  @Qualifier("queryEngine")
  private QueryEngine queryEngine;

  @Value("${metadataTests.cacheRefreshIntervalSecs}")
  private Integer testCacheRefreshIntervalSeconds;

  @Value("${metadataTests.enabled:true}")
  private Boolean testEnabled;

  @Bean(name = "testEngine")
  @Nonnull
  protected TestEngine getInstance() {
    TestPredicateEvaluator testPredicateEvaluator = TestPredicateEvaluator.getInstance();
    return new TestEngine(entityService, new TestFetcher(entityService, entitySearchService),
        new TestDefinitionProvider(testPredicateEvaluator), queryEngine, testPredicateEvaluator, 10,
        testCacheRefreshIntervalSeconds);
  }
}
