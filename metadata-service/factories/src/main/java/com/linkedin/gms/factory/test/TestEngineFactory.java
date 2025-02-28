package com.linkedin.gms.factory.test;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.entity.EntityServiceFactory;
import com.linkedin.gms.factory.search.EntitySearchServiceFactory;
import com.linkedin.gms.factory.timeseries.TimeseriesAspectServiceFactory;
import com.linkedin.metadata.config.TestsConfiguration;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

  @Bean(name = "metadataTestsActionsExecutorService")
  @Nonnull
  protected ExecutorService metadataTestsActionsExecutorService(
      @Nonnull ConfigurationProvider configurationProvider) {
    TestsConfiguration testsConfiguration = configurationProvider.getMetadataTests();
    return new ThreadPoolExecutor(
        testsConfiguration.getActions().getConcurrency(), // core threads
        testsConfiguration.getActions().getConcurrency(), // max threads
        testsConfiguration.getActions().getThreadKeepAlive(),
        TimeUnit.SECONDS, // thread keep-alive time
        new ArrayBlockingQueue<>(
            testsConfiguration.getActions().getQueueSize()), // fixed size queue
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Bean(name = "testEngine")
  @Nonnull
  protected TestEngine getInstance(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      @Nonnull final ConfigurationProvider configurationProvider,
      @Qualifier("metadataTestsActionsExecutorService") @Nonnull
          final ExecutorService actionsExecutorService) {

    PredicateEvaluator predicateEvaluator = PredicateEvaluator.getInstance();
    TestsConfiguration testsConfiguration =
        configurationProvider.getMetadataTests().toBuilder()
            // Enforce graceful shutdown
            .jvmShutdownHookEnabled(true)
            .build();

    return new TestEngine(
        systemOpContext,
        testsConfiguration,
        entityService,
        entitySearchService,
        timeseriesAspectService,
        new TestFetcher(entityService, entitySearchService),
        new TestDefinitionParser(predicateEvaluator),
        queryEngine,
        predicateEvaluator,
        this.actionApplier,
        actionsExecutorService);
  }
}
