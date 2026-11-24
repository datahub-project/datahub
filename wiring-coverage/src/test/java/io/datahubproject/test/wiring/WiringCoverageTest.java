package io.datahubproject.test.wiring;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ContextConfiguration(classes = WiringCoverageTest.RealAndMockedConfig.class)
public class WiringCoverageTest extends AbstractTestNGSpringContextTests {

  public static class RealService {}

  public static class MockedService {}

  public static class RunnerService {}

  @Configuration
  public static class RealAndMockedConfig {
    @Bean
    public RealService realService() {
      return new RealService();
    }

    @Bean
    public MockedService mockedService() {
      return new MockedService();
    }
  }

  @Configuration
  public static class RunnerConfig {
    @Bean
    public RunnerService runnerService() {
      return new RunnerService();
    }
  }

  @Configuration
  public static class NeverLoadedConfig {
    @Bean
    public RealService unusedService() {
      return new RealService();
    }
  }

  @Configuration
  public static class ParentConfig {
    @Bean
    public RealService inheritedService() {
      return new RealService();
    }
  }

  @Configuration
  public static class ChildConfig extends ParentConfig {
    @Bean
    public static RunnerService staticService() {
      return new RunnerService();
    }
  }

  /** A real @Bean method that hands out a mock: the method ran, so it counts. */
  @Configuration
  public static class MockReturningConfig {
    @Bean
    public MockedService mockReturningService() {
      return Mockito.mock(MockedService.class);
    }
  }

  @MockitoBean private MockedService mockedService;

  @Autowired private RealService realService;

  @Test
  public void recordsRealBeansButNotMockitoBeanReplacements() throws IOException {
    Set<String> lines = recorded();
    String config = RealAndMockedConfig.class.getName();
    assertTrue(lines.contains("C " + config), lines.toString());
    assertTrue(lines.contains("M " + config + "#realService"), lines.toString());
    assertFalse(lines.contains("M " + config + "#mockedService"), lines.toString());
    assertFalse(
        lines.stream().anyMatch(l -> l.contains(NeverLoadedConfig.class.getName())),
        lines.toString());
  }

  @Test
  public void recordsHandBuiltContextsThatOptIn() throws IOException {
    new ApplicationContextRunner()
        .withInitializer(WiringCoverage.initializer())
        .withUserConfiguration(RunnerConfig.class)
        .run(context -> context.getBean(RunnerService.class));
    assertTrue(
        recorded().contains("M " + RunnerConfig.class.getName() + "#runnerService"),
        recorded().toString());
  }

  @Test
  public void creditsInheritedAndStaticBeanMethodsToTheLoadedClass() throws IOException {
    new ApplicationContextRunner()
        .withInitializer(WiringCoverage.initializer())
        .withUserConfiguration(ChildConfig.class)
        .run(context -> context.getBean(RunnerService.class));
    Set<String> lines = recorded();
    String child = ChildConfig.class.getName();
    assertTrue(lines.contains("M " + child + "#inheritedService"), lines.toString());
    assertTrue(lines.contains("M " + child + "#staticService"), lines.toString());
    assertFalse(
        lines.stream().anyMatch(l -> l.startsWith("M " + ParentConfig.class.getName() + "#")),
        lines.toString());
  }

  @Test
  public void recordsRealBeanMethodsThatReturnAMock() throws IOException {
    new ApplicationContextRunner()
        .withInitializer(WiringCoverage.initializer())
        .withUserConfiguration(MockReturningConfig.class)
        .run(context -> context.getBean(MockedService.class));
    assertTrue(
        recorded().contains("M " + MockReturningConfig.class.getName() + "#mockReturningService"),
        recorded().toString());
  }

  private static Set<String> recorded() throws IOException {
    assertTrue(WiringCoverage.enabled(), "test task must set " + WiringCoverage.DIR_PROPERTY);
    Set<String> lines = new HashSet<>();
    try (Stream<Path> files =
        Files.list(Paths.get(System.getProperty(WiringCoverage.DIR_PROPERTY)))) {
      for (Path f : (Iterable<Path>) files::iterator) {
        lines.addAll(Files.readAllLines(f));
      }
    }
    return lines;
  }
}
