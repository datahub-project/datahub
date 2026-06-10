package com.linkedin.gms.factory.ratelimit;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.ratelimit.RateLimitConfigLoader;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.RateLimitFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class RateLimitEngineFactoryTest {

  private final RateLimitEngineFactory factory = new RateLimitEngineFactory();

  @Test
  public void testCreatesConfigLoaderBean() {
    OperationContext operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    RateLimitConfigLoader loader = factory.rateLimitConfigLoader(operationContext);
    assertNotNull(loader);
  }

  @Test
  public void testCreatesEngineAndFilterBeans() {
    GMSConfiguration gmsConfiguration = new GMSConfiguration();
    RateLimitProperties rateLimits = new RateLimitProperties();
    rateLimits.getCapacity().getGraphql().setPathPattern("/api/graphql");
    gmsConfiguration.setRateLimits(rateLimits);
    gmsConfiguration.setBasePathEnabled(false);

    OperationContext operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    RateLimitConfigLoader loader = factory.rateLimitConfigLoader(operationContext);

    RateLimitEngine engine =
        factory.rateLimitEngine(gmsConfiguration, null, null, operationContext, loader);
    RateLimitFilter filter = factory.rateLimitFilter(engine);

    assertNotNull(engine);
    assertNotNull(filter);
  }
}
