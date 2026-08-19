package com.linkedin.common.client.restli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.restli.client.AbstractRequestBuilder;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.testng.annotations.Test;

/**
 * Extension-point behavior for the outbound-Restli enricher seam that {@code
 * BaseClient.sendClientRequest} invokes on every client→GMS call (including the MAE/MCE consumer
 * hooks).
 *
 * <p>The safety guarantee for OSS: with no enrichers registered the resolver leaves the outbound
 * request builder untouched, so the Restli call path is unchanged. When an enricher is registered
 * it is applied — the mechanism any outbound routing-header enricher depends on.
 */
public class RestliRequestContextResolverTest {

  private final OperationContext opContext = mock(OperationContext.class);

  @Test
  public void noEnrichers_leavesRequestBuilderUntouched() {
    AbstractRequestBuilder<?, ?, ?> builder = mock(AbstractRequestBuilder.class);

    new RestliRequestContextResolver(List.of()).resolve(builder, opContext);

    verify(builder, never()).addHeader(any(), any());
  }

  @Test
  public void appliesRegisteredEnricherToRequestBuilder() {
    AbstractRequestBuilder<?, ?, ?> builder = mock(AbstractRequestBuilder.class);
    RestliRequestContextEnricher enricher = (b, ctx) -> b.addHeader("X-Test", "v");

    new RestliRequestContextResolver(List.of(enricher)).resolve(builder, opContext);

    verify(builder).addHeader("X-Test", "v");
  }
}
