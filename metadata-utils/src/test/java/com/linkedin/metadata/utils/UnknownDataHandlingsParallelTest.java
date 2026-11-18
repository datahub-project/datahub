package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.TestNG;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

/**
 * Real TestNG runs: the unknown-data handling override is refused where another test class can run
 * alongside, and allowed where classes run one at a time, whatever else the suite does.
 */
public class UnknownDataHandlingsParallelTest {

  // The nested classes are also discovered as tests on their own; they only act inside a run here.
  private static volatile boolean inNestedRun;

  @Test
  public void testRefusedWhenClassesRunInParallel() {
    assertNotEquals(run(XmlSuite.ParallelMode.CLASSES, 2, Switching.class, Other.class), 0);
  }

  @Test
  public void testRefusedWhenMethodsRunInParallel() {
    assertNotEquals(run(XmlSuite.ParallelMode.METHODS, 2, Switching.class), 0);
  }

  @Test
  public void testAllowedWhenClassesRunOneAtATime() {
    assertEquals(run(XmlSuite.ParallelMode.TESTS, 1, Other.class, Switching.class), 0);
    assertEquals(run(XmlSuite.ParallelMode.NONE, 1, Other.class, Switching.class), 0);
  }

  @Test
  public void testAllowedForTheOnlyClassOfAParallelClassesTest() {
    assertEquals(run(XmlSuite.ParallelMode.CLASSES, 5, Switching.class), 0);
  }

  @Test
  public void testRefusedWhenTheSuiteRunsTestsInParallelDespiteTheTestsOwnSetting() {
    // The suite runs both tests at once; the switching test's own "none" doesn't change that.
    XmlSuite suite = new XmlSuite();
    suite.setName("nested");
    suite.setParallel(XmlSuite.ParallelMode.TESTS);
    suite.setThreadCount(2);
    xmlTest(suite, "switching", XmlSuite.ParallelMode.NONE, 1, Switching.class);
    xmlTest(suite, "other", XmlSuite.ParallelMode.NONE, 1, Other.class);

    assertNotEquals(run(suite), 0);
  }

  /** The nested run's status: 0 when every test passed. */
  private static int run(XmlSuite.ParallelMode mode, int threads, Class<?>... classes) {
    XmlSuite suite = new XmlSuite();
    suite.setName("nested");
    xmlTest(suite, "nested-test", mode, threads, classes);
    return run(suite);
  }

  private static XmlTest xmlTest(
      XmlSuite suite, String name, XmlSuite.ParallelMode mode, int threads, Class<?>... classes) {
    XmlTest test = new XmlTest(suite);
    test.setName(name);
    test.setParallel(mode);
    test.setThreadCount(threads);
    List<XmlClass> xmlClasses =
        Arrays.stream(classes).map(XmlClass::new).collect(Collectors.toList());
    test.setXmlClasses(xmlClasses);
    return test;
  }

  private static int run(XmlSuite suite) {
    TestNG testng = new TestNG(false);
    testng.setXmlSuites(List.of(suite));
    inNestedRun = true;
    try {
      testng.run();
    } finally {
      inNestedRun = false;
      UnknownDataHandlingsForTests.reset();
    }
    assertSame(UnknownDataHandlings.handling(), StrictHandling.INSTANCE);
    return testng.getStatus();
  }

  public static class Switching {
    @BeforeMethod
    public void useTolerant() {
      if (inNestedRun) {
        UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
      }
    }

    @AfterMethod(alwaysRun = true)
    public void resetHandling() {
      UnknownDataHandlingsForTests.reset();
    }

    @Test
    public void first() {}

    @Test
    public void second() {}
  }

  public static class Other {
    @Test
    public void runs() {}
  }
}
