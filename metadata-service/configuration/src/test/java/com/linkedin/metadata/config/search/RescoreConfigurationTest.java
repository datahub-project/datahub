package com.linkedin.metadata.config.search;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.IOException;
import java.util.Map;
import org.testng.annotations.Test;

public class RescoreConfigurationTest {

  @Test
  public void testDefaultConfiguration() {
    RescoreConfiguration config = new RescoreConfiguration();
    assertFalse(config.isEnabled(), "Default enabled should be false");
    assertEquals(config.getWindowSize(), 100, "Default window size should be 100");
    assertEquals(config.getQueryWeight(), 0.0f, "Default query weight should be 0.0");
    assertEquals(
        config.getRescoreQueryWeight(), 1.0f, "Default rescore query weight should be 1.0");
    assertEquals(
        config.getFile(), "rescore_config.yaml", "Default file should be rescore_config.yaml");
  }

  @Test
  public void testBuilderConfiguration() {
    Map<String, Object> functionScore =
        Map.of(
            "functions",
            Map.of("filter", Map.of("term", Map.of("active", true))),
            "score_mode",
            "multiply");

    RescoreConfiguration config =
        RescoreConfiguration.builder()
            .enabled(true)
            .windowSize(1000)
            .queryWeight(0.6f)
            .rescoreQueryWeight(0.4f)
            .functionScore(functionScore)
            .build();

    assertTrue(config.isEnabled(), "Enabled should be true");
    assertEquals(config.getWindowSize(), 1000, "Window size should be 1000");
    assertEquals(config.getQueryWeight(), 0.6f, "Query weight should be 0.6");
    assertEquals(config.getRescoreQueryWeight(), 0.4f, "Rescore query weight should be 0.4");
    assertNotNull(config.getFunctionScore(), "Function score should not be null");
  }

  @Test
  public void testResolveWithDisabledRescore() throws IOException {
    RescoreConfiguration config = RescoreConfiguration.builder().enabled(false).build();

    ObjectMapper mapper = new YAMLMapper();
    RescoreConfiguration resolved = config.resolve(mapper);

    assertFalse(resolved.isEnabled(), "Resolved config should remain disabled");
    assertSame(resolved, config, "Should return same instance when disabled");
  }

  @Test
  public void testResolveWithNullFile() throws IOException {
    RescoreConfiguration config = RescoreConfiguration.builder().enabled(true).file(null).build();

    ObjectMapper mapper = new YAMLMapper();
    RescoreConfiguration resolved = config.resolve(mapper);

    assertTrue(resolved.isEnabled(), "Resolved config should remain enabled");
    assertSame(resolved, config, "Should return same instance when file is null");
  }

  @Test
  public void testResolveWithNonExistentFile() throws IOException {
    RescoreConfiguration config =
        RescoreConfiguration.builder().enabled(true).file("non_existent_file.yaml").build();

    ObjectMapper mapper = new YAMLMapper();
    RescoreConfiguration resolved = config.resolve(mapper);

    // Should return same instance and log warning (not throw exception)
    assertSame(resolved, config, "Should return same instance when file doesn't exist");
  }

  @Test
  public void testYamlStructure() throws IOException {
    // Test that we can parse the expected YAML structure (using camelCase property names)
    String yamlContent =
        """
        rescore:
          enabled: true
          windowSize: 300
          queryWeight: 0.8
          rescoreQueryWeight: 0.2
          functionScore:
            functions:
              - filter:
                  term:
                    hasDescription:
                      value: true
                weight: 1.5
            score_mode: multiply
            boost_mode: multiply
        """;

    ObjectMapper mapper = new YAMLMapper();
    RescoreConfiguration.RescoreConfigYaml yaml =
        mapper.readValue(yamlContent, RescoreConfiguration.RescoreConfigYaml.class);

    assertNotNull(yaml.getRescore(), "Rescore details should not be null");
    assertTrue(yaml.getRescore().isEnabled(), "Should be enabled");
    assertEquals(yaml.getRescore().getWindowSize(), 300, "Window size should be 300");
    assertEquals(yaml.getRescore().getQueryWeight(), 0.8f, "Query weight should be 0.8");
    assertEquals(
        yaml.getRescore().getRescoreQueryWeight(), 0.2f, "Rescore query weight should be 0.2");
    assertNotNull(yaml.getRescore().getFunctionScore(), "Function score should not be null");
  }

  @Test
  public void testApplyYamlConfig() throws IOException {
    String yamlContent =
        """
        rescore:
          enabled: true
          windowSize: 400
          queryWeight: 0.65
          rescoreQueryWeight: 0.35
          functionScore:
            score_mode: sum
        """;

    ObjectMapper mapper = new YAMLMapper();
    RescoreConfiguration.RescoreConfigYaml yaml =
        mapper.readValue(yamlContent, RescoreConfiguration.RescoreConfigYaml.class);

    RescoreConfiguration config = new RescoreConfiguration();
    RescoreConfiguration applied =
        config
            .builder()
            .enabled(yaml.getRescore().isEnabled())
            .windowSize(yaml.getRescore().getWindowSize())
            .queryWeight(yaml.getRescore().getQueryWeight())
            .rescoreQueryWeight(yaml.getRescore().getRescoreQueryWeight())
            .functionScore(yaml.getRescore().getFunctionScore())
            .build();

    assertTrue(applied.isEnabled(), "Applied config should be enabled");
    assertEquals(applied.getWindowSize(), 400, "Window size should be 400");
    assertEquals(applied.getQueryWeight(), 0.65f, "Query weight should be 0.65");
    assertEquals(applied.getRescoreQueryWeight(), 0.35f, "Rescore query weight should be 0.35");
    assertNotNull(applied.getFunctionScore(), "Function score should not be null");
  }

  @Test
  public void testWeightsBetweenZeroAndOne() {
    RescoreConfiguration config =
        RescoreConfiguration.builder().queryWeight(0.7f).rescoreQueryWeight(0.3f).build();

    assertTrue(
        config.getQueryWeight() >= 0 && config.getQueryWeight() <= 1,
        "Query weight should be between 0 and 1");
    assertTrue(
        config.getRescoreQueryWeight() >= 0 && config.getRescoreQueryWeight() <= 1,
        "Rescore query weight should be between 0 and 1");
  }

  @Test
  public void testFunctionScoreStructure() {
    Map<String, Object> functionScore =
        Map.of(
            "functions",
                Map.of(
                    "filter",
                    Map.of("term", Map.of("hasOwners", Map.of("value", true))),
                    "weight",
                    1.3),
            "score_mode", "multiply",
            "boost_mode", "multiply");

    RescoreConfiguration config =
        RescoreConfiguration.builder().enabled(true).functionScore(functionScore).build();

    assertNotNull(config.getFunctionScore(), "Function score should not be null");
    assertTrue(config.getFunctionScore().containsKey("functions"), "Should contain functions key");
    assertTrue(
        config.getFunctionScore().containsKey("score_mode"), "Should contain score_mode key");
    assertTrue(
        config.getFunctionScore().containsKey("boost_mode"), "Should contain boost_mode key");
  }
}
