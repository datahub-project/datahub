package com.linkedin.gms.factory.ratelimit;

import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.RateLimitFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.springframework.core.env.StandardEnvironment;
import org.testng.annotations.Test;

public class RateLimitEngineFactoryTest {

  private final RateLimitEngineFactory factory = new RateLimitEngineFactory();

  @Test
  public void testCreatesEngineAndFilterBeans() {
    GMSConfiguration gmsConfiguration = new GMSConfiguration();
    RateLimitProperties rateLimits = new RateLimitProperties();
    rateLimits.getCapacity().getGraphql().setPathPattern("/api/graphql");
    gmsConfiguration.setRateLimits(rateLimits);
    gmsConfiguration.setBasePathEnabled(false);

    OperationContext operationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    RateLimitEngine engine =
        factory.rateLimitEngine(
            gmsConfiguration, null, null, operationContext, new StandardEnvironment());
    RateLimitFilter filter = factory.rateLimitFilter(engine, operationContext);

    assertNotNull(engine);
    assertNotNull(filter);
  }
}
