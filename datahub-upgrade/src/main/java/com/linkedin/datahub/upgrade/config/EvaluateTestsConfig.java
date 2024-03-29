package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.test.EvaluateTests;
import com.linkedin.entity.client.SystemEntityClient;
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
import org.springframework.context.annotation.DependsOn;

@Configuration
@Conditional(EvaluateTestsCondition.class)
public class EvaluateTestsConfig {
  @Autowired ApplicationContext applicationContext;

  @Bean(name = "evaluateTests")
  @DependsOn({"entitySearchService", "testEngine", "systemOperationContext"})
  @Nonnull
  public EvaluateTests createInstance(
      @Qualifier("systemEntityClient") SystemEntityClient entityClient,
      EntitySearchService entitySearchService,
      TestEngine testEngine,
      @Qualifier("systemOperationContext") OperationContext systemOpContext) {
    return new EvaluateTests(systemOpContext, entityClient, entitySearchService, testEngine);
  }
}
