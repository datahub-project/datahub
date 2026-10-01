package com.linkedin.gms.factory.runtime;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.runtime.RuntimeRole;
import org.springframework.boot.SpringApplication;
import org.springframework.mock.env.MockEnvironment;
import org.testng.annotations.Test;

public class StandaloneConsumerRuntimeRolePostProcessorTest {

  private final StandaloneConsumerRuntimeRolePostProcessor consumer =
      new StandaloneConsumerRuntimeRolePostProcessor();
  private final RuntimeRolePolicyPostProcessor policy = new RuntimeRolePolicyPostProcessor();

  @Test
  public void forcesClientBeforeTheCachePolicyRuns() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(RuntimeRole.PROPERTY, "service");
    environment.setProperty("datahub.gms.entityGraphCache.enabled", "true");

    consumer.postProcessEnvironment(environment, new SpringApplication());
    policy.postProcessEnvironment(environment, new SpringApplication());

    assertEquals(environment.getProperty(RuntimeRole.PROPERTY), "client");
    assertEquals(environment.getProperty("datahub.gms.entityGraphCache.enabled"), "false");
    assertTrue(
        environment
            .getPropertySources()
            .contains(StandaloneConsumerRuntimeRolePostProcessor.PROPERTY_SOURCE_NAME));
  }
}
