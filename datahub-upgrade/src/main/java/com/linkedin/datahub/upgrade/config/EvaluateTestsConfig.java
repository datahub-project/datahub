package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.test.EvaluateTests;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.test.TestEngineFactory;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.config.TestsHookConfiguration;
import com.linkedin.metadata.config.TestsHookExecutionLimitConfiguration;
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
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@Configuration
@Conditional(EvaluateTestsCondition.class)
@Import(TestEngineFactory.class)
public class EvaluateTestsConfig {
  @Autowired ApplicationContext applicationContext;

  @Bean(name = "evaluateTests")
  @Nonnull
  public EvaluateTests createInstance(
      @Qualifier("systemEntityClient") final SystemEntityClient entityClient,
      @Qualifier("entitySearchService") final EntitySearchService entitySearchService,
      @Qualifier("testEngine") final TestEngine testEngine,
      @Qualifier("systemOperationContext") final OperationContext systemOpContext) {
    return new EvaluateTests(systemOpContext, entityClient, entitySearchService, testEngine);
  }

  @Primary
  @Bean(name = "testEngine")
  @Nonnull
  protected TestEngine testEngine(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext,
      @Nonnull ConfigurationProvider configurationProvider,
      @Qualifier("entityService") EntityService<?> entityService,
      @Qualifier("entitySearchService") EntitySearchService entitySearchService,
      @Qualifier("timeseriesAspectService") TimeseriesAspectService timeseriesAspectService,
      @Qualifier("queryEngine") QueryEngine queryEngine,
      @Qualifier("testActionApplier") ActionApplier actionApplier) {

    PredicateEvaluator predicateEvaluator = PredicateEvaluator.getInstance();
    TestsConfiguration testsConfiguration = configurationProvider.getMetadataTests();
    TestsHookConfiguration hookConfiguration = testsConfiguration.getHook();
    TestsHookExecutionLimitConfiguration testsHookExecutionLimitConfiguration =
        hookConfiguration.getHookExecutionLimit();
    return new TestEngine(
        systemOpContext,
        testsConfiguration.isEnabled() || hookConfiguration.isEnabled(),
        entityService,
        entitySearchService,
        timeseriesAspectService,
        new TestFetcher(entityService, entitySearchService),
        new TestDefinitionParser(predicateEvaluator),
        queryEngine,
        predicateEvaluator,
        actionApplier,
        0,
        0,
        testsConfiguration.getElasticSearchExecutor().isEnabled(),
        testsHookExecutionLimitConfiguration);
  }
}
