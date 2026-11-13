package com.linkedin.metadata.utils;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.ServiceLoader;
import org.testng.ITestClass;
import org.testng.ITestNGListener;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UnknownDataHandlingsTest {

  private static final UnknownDataHandling ENTITY_ONLY =
      TolerantHandling.of(true, false, t -> true);

  @BeforeMethod
  @AfterMethod(alwaysRun = true)
  public void startUninstalled() {
    UnknownDataHandlingsForTests.reset();
    UnknownDataHandlingsForTests.uninstall();
  }

  @Test
  public void testTestOverrideLeavesTheInstalledHandling() {
    UnknownDataHandlings.install(ENTITY_ONLY);

    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
    assertSame(UnknownDataHandlings.handling(), TolerantHandling.INSTANCE);

    UnknownDataHandlingsForTests.reset();
    assertSame(UnknownDataHandlings.handling(), ENTITY_ONLY);
    // Still installed: different flags still fail.
    assertThrows(
        IllegalStateException.class,
        () -> UnknownDataHandlings.install(TolerantHandling.of(false, true, t -> true)));
  }

  @Test
  public void testStrictUntilInstalled() {
    assertSame(UnknownDataHandlings.handling(), StrictHandling.INSTANCE);

    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
    UnknownDataHandlingsForTests.reset();

    assertSame(UnknownDataHandlings.handling(), StrictHandling.INSTANCE);
  }

  @Test
  public void testInstallingTheSameFlagsAgainKeepsTheFirstHandling() {
    UnknownDataHandlings.install(ENTITY_ONLY);

    UnknownDataHandlings.install(TolerantHandling.of(true, false, t -> false));

    assertSame(UnknownDataHandlings.handling(), ENTITY_ONLY);
  }

  @Test
  public void testListenerRemovesAClassOverrideAndKeepsTheInstalledHandling() {
    UnknownDataHandlings.install(ENTITY_ONLY);
    // The class switches and never resets.
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);

    new UnknownDataHandlingIsolationListener().onAfterClass(mock(ITestClass.class));

    assertSame(UnknownDataHandlings.handling(), ENTITY_ONLY);
  }

  @Test
  public void testListenerIsLoadedForEveryTestRun() {
    // TestNG loads listeners through META-INF/services: without the entry, nothing resets.
    boolean registered = false;
    for (ITestNGListener listener : ServiceLoader.load(ITestNGListener.class)) {
      registered |= listener instanceof UnknownDataHandlingIsolationListener;
    }
    assertTrue(registered, "UnknownDataHandlingIsolationListener isn't in META-INF/services");
  }
}
