package com.linkedin.metadata.utils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

/**
 * The test switch for {@link UnknownDataHandlings}, kept in test fixtures so production code can't
 * reach it. The override is one value per JVM, so {@link #use} refuses it in a TestNG test that
 * runs other test classes alongside, which would see it; {@link
 * UnknownDataHandlingIsolationListener} removes it after every class.
 */
public final class UnknownDataHandlingsForTests {

  private UnknownDataHandlingsForTests() {}

  /**
   * Overrides the installed handling until {@link #reset}.
   *
   * @throws IllegalStateException when called from a TestNG test that runs other classes alongside
   *     this one; run classes that switch the handling one at a time (in metadata-io, outside the
   *     parallel suites)
   */
  public static void use(@Nonnull final UnknownDataHandling handling) {
    final ITestContext context = currentTestContext();
    if (context != null && runsClassesAlongside(context)) {
      throw new IllegalStateException(
          context.getCurrentXmlTest().getName()
              + " runs other test classes alongside this one; the unknown-data handling override"
              + " would reach them. Run classes that switch it one at a time.");
    }
    UnknownDataHandlings.use(handling);
  }

  /** Removes the override, going back to the installed handling (strict if none). */
  public static void reset() {
    UnknownDataHandlings.reset();
  }

  /**
   * For tests of {@link UnknownDataHandlings#install} itself, which model several process startups
   * in one JVM: forgets the installed handling so the next install starts afresh.
   */
  public static void uninstall() {
    UnknownDataHandlings.uninstall();
  }

  /** The TestNG test this thread is running, or null outside a TestNG test. */
  @Nullable
  private static ITestContext currentTestContext() {
    final ITestResult result = Reporter.getCurrentTestResult();
    return result == null ? null : result.getTestContext();
  }

  /** Whether the test's parallel settings can run another test class while this one runs. */
  static boolean runsClassesAlongside(@Nonnull final ITestContext context) {
    final XmlTest test = context.getCurrentXmlTest();
    // The suite running its tests in parallel puts other tests' classes alongside, whatever this
    // test's own setting (a test's parallel mode and thread count override the suite's).
    final XmlSuite suite = test.getSuite();
    if (suite.getParallel() == XmlSuite.ParallelMode.TESTS
        && suite.getThreadCount() > 1
        && suite.getTests().size() > 1) {
      return true;
    }
    final XmlSuite.ParallelMode mode = test.getParallel();
    if (mode == null || !mode.isParallel() || test.getThreadCount() <= 1) {
      return false;
    }
    switch (mode) {
      case TESTS:
        return test.getSuite().getTests().size() > 1;
      case CLASSES:
      case INSTANCES:
        // As the test declares it: a single class and no packages has nothing to run alongside.
        return !test.getXmlPackages().isEmpty() || test.getXmlClasses().size() > 1;
      default:
        return true;
    }
  }
}
