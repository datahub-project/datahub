package com.linkedin.gms.factory.test;

import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.timeseries.TimeseriesAspectServiceFactory;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.TestFetcher;
import com.linkedin.metadata.test.action.ActionApplier;
import com.linkedin.metadata.test.definition.TestDefinitionParser;
import com.linkedin.metadata.test.eval.PredicateEvaluator;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({
  EntityServiceFactory.class,
  EntitySearchServiceFactory.class,
  TimeseriesAspectServiceFactory.class
})
public class TestEngineFactory {

  @Autowired
  @Qualifier("entityService")
  private EntityService<?> entityService;

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService entitySearchService;

  @Autowired
  @Qualifier("timeseriesAspectService")
  private TimeseriesAspectService timeseriesAspectService;

  @Autowired
  @Qualifier("queryEngine")
  private QueryEngine queryEngine;

  @Autowired
  @Qualifier("testActionApplier")
  private ActionApplier actionApplier;

  @Value("${metadataTests.cacheRefreshIntervalSecs}")
  private Integer testCacheRefreshIntervalSeconds;

  @Bean(name = "testEngine")
  @Nonnull
  protected TestEngine getInstance(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext) {

    PredicateEvaluator predicateEvaluator = PredicateEvaluator.getInstance();
    return new TestEngine(
        systemOpContext,
        entityService,
        entitySearchService,
        timeseriesAspectService,
        new TestFetcher(entityService, entitySearchService),
        new TestDefinitionParser(predicateEvaluator),
        queryEngine,
        predicateEvaluator,
        this.actionApplier,
        10,
        testCacheRefreshIntervalSeconds);
  }
}
