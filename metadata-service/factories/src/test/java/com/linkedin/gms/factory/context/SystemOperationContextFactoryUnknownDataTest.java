package com.linkedin.gms.factory.context;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertSame;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class SystemOperationContextFactoryUnknownDataTest {

  @AfterMethod
  public void tearDown() {
    UnknownDataHandlings.reset();
  }

  @Test
  public void testFlagSelectsHowUnknownDataIsHandled() {
    SystemOperationContextFactory.configureUnknownDataHandling(provider(false));
    assertSame(UnknownDataHandlings.handling(), StrictHandling.INSTANCE);

    SystemOperationContextFactory.configureUnknownDataHandling(provider(true));
    assertSame(UnknownDataHandlings.handling(), TolerantHandling.INSTANCE);
  }

  private static ConfigurationProvider provider(final boolean tolerant) {
    FeatureFlags flags = new FeatureFlags();
    flags.setUnknownDataToleranceEnabled(tolerant);
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    when(provider.getFeatureFlags()).thenReturn(flags);
    return provider;
  }
}
