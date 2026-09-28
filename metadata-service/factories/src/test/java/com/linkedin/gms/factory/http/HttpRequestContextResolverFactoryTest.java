package com.linkedin.gms.factory.http;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.http.HttpRequestContextEnricher;
import io.datahubproject.metadata.context.http.HttpRequestContextResolver;
import java.util.Map;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.Test;

/**
 * Verifies the outbound-HTTP resolver bean actually loads and wires. The factory lives in a package
 * that is not on the restrictive GMS/MAE/MCE component-scan allowlists, so it is pulled into
 * consumer contexts via {@code @Import}; these tests exercise the factory in isolation to prove the
 * three properties every consumer relies on:
 *
 * <ul>
 *   <li>the factory registers a {@link HttpRequestContextResolver} bean (a consumer @Import-ing it
 *       gets a satisfiable dependency);
 *   <li>with no enrichers registered it is a pass-through — the OSS / no-functional-change path;
 *   <li>enrichers contributed as beans are collected and composed (the mechanism any registered
 *       outbound-header enricher depends on).
 * </ul>
 */
public class HttpRequestContextResolverFactoryTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner().withUserConfiguration(HttpRequestContextResolverFactory.class);

  @Test
  public void registersResolverBean_passThroughWhenNoEnrichers() {
    runner.run(
        ctx -> {
          HttpRequestContextResolver resolver = ctx.getBean(HttpRequestContextResolver.class);
          assertTrue(resolver.resolveHeaders(mock(OperationContext.class)).isEmpty());
        });
  }

  @Test
  public void composesEnrichersDiscoveredAsBeans() {
    runner
        .withUserConfiguration(TestEnricherConfig.class)
        .run(
            ctx -> {
              HttpRequestContextResolver resolver = ctx.getBean(HttpRequestContextResolver.class);
              Map<String, String> headers = resolver.resolveHeaders(mock(OperationContext.class));
              assertEquals(headers.get("X-Test"), "v");
            });
  }

  @Configuration
  static class TestEnricherConfig {
    @Bean
    HttpRequestContextEnricher testEnricher() {
      return ctx -> Map.of("X-Test", "v");
    }
  }
}
