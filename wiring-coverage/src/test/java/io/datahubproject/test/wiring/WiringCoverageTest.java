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
