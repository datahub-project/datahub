package com.linkedin.gms.factory.context;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlings;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class SystemOperationContextFactoryUnknownDataTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @AfterMethod
  public void tearDown() {
    UnknownDataHandlings.reset();
  }

  @Test
  public void testFlagSelectsHowUnknownDataIsHandled() {
    SystemOperationContextFactory.configureUnknownDataHandling(provider(false), opContext);
    assertSame(UnknownDataHandlings.handling(), StrictHandling.INSTANCE);

    SystemOperationContextFactory.configureUnknownDataHandling(provider(true), opContext);
    assertTrue(UnknownDataHandlings.handling() instanceof TolerantHandling);
  }

  private static ConfigurationProvider provider(final boolean tolerant) {
    FeatureFlags flags = new FeatureFlags();
    flags.setUnknownDataToleranceEnabled(tolerant);
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    when(provider.getFeatureFlags()).thenReturn(flags);
    return provider;
  }
}
