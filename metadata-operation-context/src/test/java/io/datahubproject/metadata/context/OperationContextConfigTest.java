package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.metadata.context.usage.UsageOperation;
import io.datahubproject.metadata.context.usage.instrumentation.SessionContextEnricher;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class OperationContextConfigTest {

  @Test
  public void testCacheKeyWithoutEnricher() {
    ViewAuthorizationConfiguration viewAuth =
        ViewAuthorizationConfiguration.builder().enabled(false).build();

    OperationContextConfig config =
        OperationContextConfig.builder().viewAuthorizationConfiguration(viewAuth).build();

    assertEquals(config.getCacheKeyComponent().orElseThrow(), Integer.valueOf(viewAuth.hashCode()));
  }

  @Test
  public void testCacheKeyIncludesEnricherClass() {
    ViewAuthorizationConfiguration viewAuth =
        ViewAuthorizationConfiguration.builder().enabled(false).build();
    SessionContextEnricher enricher = noopEnricher();

    OperationContextConfig without =
        OperationContextConfig.builder().viewAuthorizationConfiguration(viewAuth).build();
    OperationContextConfig with =
        OperationContextConfig.builder()
            .viewAuthorizationConfiguration(viewAuth)
            .sessionContextEnricher(enricher)
            .build();

    assertNotEquals(without.getCacheKeyComponent(), with.getCacheKeyComponent());
    assertTrue(with.getSessionContextEnricher() == enricher);
  }

  @Test
  public void testAsSessionInvokesEnricherHooks() {
    AtomicInteger beforeBuild = new AtomicInteger();
    AtomicInteger onReady = new AtomicInteger();
    SessionContextEnricher enricher =
        new SessionContextEnricher() {
          @Override
          public void enrichBeforeBuild(
              RequestContext.RequestContextBuilder requestBuilder,
              Authentication sessionAuthentication) {
            beforeBuild.incrementAndGet();
            requestBuilder.withUsageOperation(UsageOperation.METADATA_READ);
          }

          @Override
          public void onSessionReady(OperationContext sessionContext) {
            onReady.incrementAndGet();
          }
        };

    Authentication systemAuth = new Authentication(new Actor(ActorType.USER, "SYSTEM"), "");
    Authentication userAuth = new Authentication(new Actor(ActorType.USER, "USER"), "");

    OperationContext systemOpContext =
        OperationContext.asSystem(
            OperationContextConfig.builder().sessionContextEnricher(enricher).build(),
            systemAuth,
            Mockito.mock(EntityRegistry.class),
            Mockito.mock(ServicesRegistryContext.class),
            null,
            TestOperationContexts.emptyActiveUsersRetrieverContext(null),
            Mockito.mock(ValidationContext.class),
            null,
            true);

    OperationContext session =
        systemOpContext.asSession(
            RequestContext.builder().buildRestli("urn:li:corpuser:USER", null, "test"),
            Authorizer.EMPTY,
            userAuth);

    assertEquals(beforeBuild.get(), 1);
    assertEquals(onReady.get(), 1);
    assertEquals(
        session.getRequestContext().getUsageOperation(), UsageOperation.METADATA_READ.key());
  }

  private static SessionContextEnricher noopEnricher() {
    return new SessionContextEnricher() {
      @Override
      public void enrichBeforeBuild(
          RequestContext.RequestContextBuilder requestBuilder,
          Authentication sessionAuthentication) {}

      @Override
      public void onSessionReady(OperationContext sessionContext) {}
    };
  }
}
