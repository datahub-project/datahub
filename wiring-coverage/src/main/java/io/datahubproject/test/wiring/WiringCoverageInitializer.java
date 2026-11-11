package io.datahubproject.test.wiring;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Attaches the {@link WiringCoverage} recorder to every {@code SpringApplication} started in a test
 * JVM (e.g. {@code SpringApplicationBuilder}). Registered in {@code META-INF/spring.factories}.
 */
public class WiringCoverageInitializer
    implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  @Override
  public void initialize(ConfigurableApplicationContext context) {
    WiringCoverage.attach(context);
  }
}
