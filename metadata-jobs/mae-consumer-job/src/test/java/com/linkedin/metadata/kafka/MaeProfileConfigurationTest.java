package com.linkedin.metadata.kafka;

import static org.testng.Assert.assertEquals;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.Test;

/** Loads {@code application-mae.yaml} without scanning GMS factories. */
public class MaeProfileConfigurationTest {

  @Test
  public void testGraphCacheDefaultsOff() {
    assertEquals(graphCacheEnabled(null), "false");
  }

  @Test
  public void testGraphCacheEnvOverridesProfile() {
    assertEquals(graphCacheEnabled("true"), "true");
  }

  private static String graphCacheEnabled(String entityGraphCacheEnabled) {
    String[] defaults =
        entityGraphCacheEnabled == null
            ? new String[] {"spring.main.banner-mode=off"}
            : new String[] {
              "spring.main.banner-mode=off", "ENTITY_GRAPH_CACHE_ENABLED=" + entityGraphCacheEnabled
            };
    SpringApplicationBuilder builder =
        new SpringApplicationBuilder(Probe.class)
            .web(WebApplicationType.NONE)
            .profiles("mae")
            .properties(defaults);
    try (ConfigurableApplicationContext context = builder.run()) {
      return context.getEnvironment().getProperty("datahub.gms.entityGraphCache.enabled");
    }
  }

  @Configuration
  static class Probe {}
}
