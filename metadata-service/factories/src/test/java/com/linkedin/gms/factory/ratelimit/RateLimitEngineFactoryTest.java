package com.linkedin.gms.factory.ratelimit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.config.ratelimit.RateLimitEffectiveConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitExcludedPathProvider;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.RateLimitEngine;
import com.linkedin.metadata.ratelimit.RateLimitFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.core.env.StandardEnvironment;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class RateLimitEngineFactoryTest {

  private final RateLimitEngineFactory factory = new RateLimitEngineFactory();

  @AfterMethod
  public void tearDown() {
    RateLimitEffectiveConfig.reset();
  }

  @Test
  public void testCreatesEngineAndFilterBeans() {
    GMSConfiguration gmsConfiguration = new GMSConfiguration();
    RateLimitProperties rateLimits = new RateLimitProperties();
    rateLimits.getCapacity().getGraphql().setPathPattern("/api/graphql");
    gmsConfiguration.setRateLimits(rateLimits);
    gmsConfiguration.setBasePathEnabled(false);

    OperationContext operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    StandardEnvironment environment = new StandardEnvironment();
    RateLimitProperties effective =
        factory.effectiveRateLimitProperties(gmsConfiguration, environment);

    RateLimitEngine engine =
        factory.rateLimitEngine(
            gmsConfiguration, null, null, operationContext, environment, effective);
    RateLimitFilter filter =
        factory.rateLimitFilter(
            engine,
            operationContext,
            new StaticListableBeanFactory().getBeanProvider(RateLimitExcludedPathProvider.class));

    assertNotNull(engine);
    assertNotNull(filter);
  }

  // Catches: the factory copying the providers' paths once when it builds the filter, so a path a
  // provider names only after startup never leaves rate limiting.
  @Test
  public void providerPathsChangedAfterTheFilterIsBuiltAreHonoured() throws Exception {
    final String path = "/openapi/example/excluded";
    final RateLimitEngine engine = mock(RateLimitEngine.class);
    when(engine.isEnabled()).thenReturn(true);
    final AtomicReference<Set<String>> paths = new AtomicReference<>(Set.of());
    final StaticListableBeanFactory beans = new StaticListableBeanFactory();
    beans.addBean("provider", (RateLimitExcludedPathProvider) paths::get);
    final RateLimitFilter filter =
        factory.rateLimitFilter(
            engine,
            TestOperationContexts.systemContextNoSearchAuthorization(),
            beans.getBeanProvider(RateLimitExcludedPathProvider.class));
    paths.set(Set.of(path));
    final HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getServletPath()).thenReturn(path);
    when(request.getRequestURI()).thenReturn(path);
    when(request.getMethod()).thenReturn("POST");

    filter.doFilter(request, mock(HttpServletResponse.class), (req, resp) -> {});

    verify(engine, never()).evaluateAndAcquireRest(any(), any(), any(), any());
  }
}
