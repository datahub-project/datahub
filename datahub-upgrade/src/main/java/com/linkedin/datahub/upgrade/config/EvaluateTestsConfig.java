package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.test.EvaluateTests;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.test.TestEngineFactory;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

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
}
