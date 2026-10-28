package com.linkedin.gms.factory.context;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandling;
import com.linkedin.metadata.utils.UnknownDataHandlings;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SystemOperationContextFactoryUnknownDataTest {

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlings.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void tearDown() {
    UnknownDataHandlings.reset();
  }

  @Test
  public void testFlagsSelectHowUnknownDataIsHandled() {
    SystemOperationContextFactory.configureUnknownDataHandling(provider(false), opContext);
    assertSame(UnknownDataHandlings.handling(), StrictHandling.INSTANCE);

    UnknownDataHandlings.reset();
    SystemOperationContextFactory.configureUnknownDataHandling(provider(true), opContext);
    assertTrue(UnknownDataHandlings.handling().toleratesUnknownEntityAspect());
    assertTrue(UnknownDataHandlings.handling().toleratesUnknownEnum());
  }

  @Test
  public void testEachFlagTurnsOnOnlyItsKind() {
    SystemOperationContextFactory.configureUnknownDataHandling(provider(true, false), opContext);
    assertTrue(UnknownDataHandlings.handling().toleratesUnknownEntityAspect());
    assertFalse(UnknownDataHandlings.handling().toleratesUnknownEnum());

    UnknownDataHandlings.reset();
    SystemOperationContextFactory.configureUnknownDataHandling(provider(false, true), opContext);
    assertFalse(UnknownDataHandlings.handling().toleratesUnknownEntityAspect());
    assertTrue(UnknownDataHandlings.handling().toleratesUnknownEnum());
  }

  @Test
  public void testProcessWithoutThisContextFollowsTheFlagToo() {
    // Entity types a newer version registered are looked up by the caller.
    UnknownDataHandling handling =
        SystemOperationContextFactory.configureUnknownDataHandling(
            provider(true), urn -> urn.getId().equals("datahub.zduRegisteredType"));
    assertSame(UnknownDataHandlings.handling(), handling);
    assertTrue(handling.isUnknown(opContext.getEntityRegistry(), "zduRegisteredType", null));
    assertFalse(handling.isUnknown(opContext.getEntityRegistry(), "zduTypo", null));

    UnknownDataHandlings.reset();
    assertSame(
        SystemOperationContextFactory.configureUnknownDataHandling(provider(false), urn -> true),
        StrictHandling.INSTANCE);
  }

  @Test
  public void testInstallingDifferentFlagsAgainFails() {
    // A process builds the system context more than once with the same flags; different ones would
    // change the handling under every caller already using it.
    SystemOperationContextFactory.configureUnknownDataHandling(provider(true), opContext);
    SystemOperationContextFactory.configureUnknownDataHandling(provider(true), opContext);

    assertThrows(
        IllegalStateException.class,
        () ->
            SystemOperationContextFactory.configureUnknownDataHandling(provider(false), opContext));
    assertTrue(UnknownDataHandlings.handling().toleratesUnknownEntityAspect());
  }

  private static ConfigurationProvider provider(final boolean tolerant) {
    return provider(tolerant, tolerant);
  }

  private static ConfigurationProvider provider(
      final boolean unknownEntityAspectTolerance, final boolean unknownEnumTolerance) {
    FeatureFlags flags = new FeatureFlags();
    flags.setUnknownEntityAspectToleranceEnabled(unknownEntityAspectTolerance);
    flags.setUnknownEnumToleranceEnabled(unknownEnumTolerance);
    ConfigurationProvider provider = mock(ConfigurationProvider.class);
    when(provider.getFeatureFlags()).thenReturn(flags);
    return provider;
  }
}
