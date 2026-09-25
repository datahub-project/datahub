package com.linkedin.gms.factory.restli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.common.client.restli.RestliRequestContextEnricher;
import com.linkedin.common.client.restli.RestliRequestContextResolver;
import com.linkedin.restli.client.AbstractRequestBuilder;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testng.annotations.Test;

/**
 * Verifies the outbound-Restli resolver bean loads and wires. Same rationale as {@link
 * com.linkedin.gms.factory.http.HttpRequestContextResolverFactoryTest}: the factory is pulled into
 * consumer contexts (RestliEntityClientFactory, UsageClientFactory, ...) via {@code @Import}, so
 * this proves the bean is registered, is a pass-through with no enrichers, and composes enrichers
 * discovered as beans.
 */
public class RestliRequestContextResolverFactoryTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withUserConfiguration(RestliRequestContextResolverFactory.class);

  @Test
  public void registersResolverBean_passThroughWhenNoEnrichers() {
    runner.run(
        ctx -> {
          RestliRequestContextResolver resolver = ctx.getBean(RestliRequestContextResolver.class);
          AbstractRequestBuilder<?, ?, ?> builder = mock(AbstractRequestBuilder.class);
          resolver.resolve(builder, mock(OperationContext.class));
          verify(builder, never()).addHeader(any(), any());
        });
  }

  @Test
  public void composesEnrichersDiscoveredAsBeans() {
    runner
        .withUserConfiguration(TestEnricherConfig.class)
        .run(
            ctx -> {
              RestliRequestContextResolver resolver =
                  ctx.getBean(RestliRequestContextResolver.class);
              AbstractRequestBuilder<?, ?, ?> builder = mock(AbstractRequestBuilder.class);
              resolver.resolve(builder, mock(OperationContext.class));
              verify(builder).addHeader("X-Test", "v");
            });
  }

  @Configuration
  static class TestEnricherConfig {
    @Bean
    RestliRequestContextEnricher testEnricher() {
      return (builder, ctx) -> builder.addHeader("X-Test", "v");
    }
  }
}
