package com.linkedin.datahub.upgrade.config;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.metadata.config.runtime.RuntimeRole;
import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.core.io.support.SpringFactoriesLoader;
import org.springframework.mock.env.MockEnvironment;
import org.testng.annotations.Test;

public class UpgradeRuntimeRolePostProcessorTest {

  private final UpgradeRuntimeRolePostProcessor processor = new UpgradeRuntimeRolePostProcessor();

  @Test
  public void registeredForSpringBootStartup() {
    assertTrue(
        SpringFactoriesLoader.loadFactoryNames(
                EnvironmentPostProcessor.class,
                UpgradeRuntimeRolePostProcessor.class.getClassLoader())
            .contains(UpgradeRuntimeRolePostProcessor.class.getName()));
  }

  @Test
  public void upgradeRoleIsAccepted() {
    processor.postProcessEnvironment(environment("upgrade"), new SpringApplication());
  }

  @Test
  public void clientRoleIsAccepted() {
    processor.postProcessEnvironment(environment("client"), new SpringApplication());
  }

  @Test
  public void serviceRoleIsRejected() {
    IllegalStateException error =
        expectThrows(
            IllegalStateException.class,
            () ->
                processor.postProcessEnvironment(environment("service"), new SpringApplication()));
    assertTrue(error.getMessage().contains("accepts datahub.runtime.role client or upgrade"));
  }

  @Test
  public void missingRoleIsServiceAndRejected() {
    expectThrows(
        IllegalStateException.class,
        () -> processor.postProcessEnvironment(new MockEnvironment(), new SpringApplication()));
  }

  private static MockEnvironment environment(String role) {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(RuntimeRole.PROPERTY, role);
    return environment;
  }
}
