package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.test.EvaluateTests;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class EvaluateTestsConfig {
  @Autowired ApplicationContext applicationContext;

  @Bean(name = "evaluateTests")
  @DependsOn({"entitySearchService", "testEngine", "systemOperationContext"})
  @Nonnull
  public EvaluateTests createInstance() {
    final SystemEntityClient entityClient =
        applicationContext.getBean("systemEntityClient", SystemEntityClient.class);
    final EntitySearchService entitySearchService =
        applicationContext.getBean(EntitySearchService.class);
    final TestEngine testEngine = applicationContext.getBean(TestEngine.class);
    final OperationContext systemOpContext =
        applicationContext.getBean("systemOperationContext", OperationContext.class);
    return new EvaluateTests(systemOpContext, entityClient, entitySearchService, testEngine);
  }
}
