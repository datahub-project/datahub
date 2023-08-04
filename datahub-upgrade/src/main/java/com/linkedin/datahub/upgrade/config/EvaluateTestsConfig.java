package com.linkedin.datahub.upgrade.config;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.upgrade.test.EvaluateTests;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.test.TestEngine;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class EvaluateTestsConfig {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "evaluateTests")
  @DependsOn({"entitySearchService", "testEngine", "restliEntityClient", "systemAuthentication"})
  @Nonnull
  public EvaluateTests createInstance() {
    final EntityClient entityClient = applicationContext.getBean(RestliEntityClient.class);
    final EntitySearchService entitySearchService = applicationContext.getBean(EntitySearchService.class);
    final TestEngine testEngine = applicationContext.getBean(TestEngine.class);
    final Authentication systemAuthentication = applicationContext.getBean(Authentication.class);
    return new EvaluateTests(entityClient, entitySearchService, testEngine, systemAuthentication);
  }
}
