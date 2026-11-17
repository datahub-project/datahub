package com.linkedin.metadata.config;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.testng.annotations.Test;

/**
 * UNKNOWN_DATA_TOLERANCE_ENABLED turns on both kinds of unknown-data tolerance, and each kind's own
 * env var overrides it, as resolved from the shipped {@code application.yaml}.
 */
public class UnknownDataToleranceFlagsBindingTest {

  private static List<Boolean> resolve(final Map<String, Object> env) throws Exception {
    final StandardEnvironment environment = new StandardEnvironment();
    environment.getPropertySources().addFirst(new MapPropertySource("test-env", env));
    new YamlPropertySourceLoader()
        .load("application", new ClassPathResource("application.yaml"))
        .forEach(environment.getPropertySources()::addLast);
    return List.of(
        environment.getProperty("featureFlags.unknownEntityAspectToleranceEnabled", Boolean.class),
        environment.getProperty("featureFlags.unknownEnumToleranceEnabled", Boolean.class));
  }

  @Test
  public void testBothOffByDefault() throws Exception {
    assertEquals(resolve(Map.of()), List.of(false, false));
  }

  @Test
  public void testUmbrellaTurnsOnBoth() throws Exception {
    assertEquals(resolve(Map.of("UNKNOWN_DATA_TOLERANCE_ENABLED", "true")), List.of(true, true));
  }

  @Test
  public void testEachKindOverridesTheUmbrella() throws Exception {
    assertEquals(
        resolve(
            Map.of(
                "UNKNOWN_DATA_TOLERANCE_ENABLED", "true",
                "UNKNOWN_ENTITY_ASPECT_TOLERANCE_ENABLED", "false")),
        List.of(false, true));
    assertEquals(resolve(Map.of("UNKNOWN_ENUM_TOLERANCE_ENABLED", "true")), List.of(false, true));
  }
}
