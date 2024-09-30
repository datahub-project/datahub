package com.linkedin.metadata.search.fixtures;

import static org.testng.AssertJUnit.assertEquals;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class SampleDataFixtureSetupTest extends AbstractTestNGSpringContextTests {
  private static final String DEFAULT_CONFIG = "search_config.yaml";
  private static final String TEST_FIXTURE_CONFIG = "search_config_fixture_test.yml";
  private static final YAMLMapper MAPPER = new YAMLMapper();

  /**
   * Ensure default search configuration matches the test fixture configuration (allowing for some
   * differences)
   */
  @Test
  public void testConfig() throws IOException {
    final CustomSearchConfiguration defaultConfig;
    final CustomSearchConfiguration fixtureConfig;

    try (InputStream stream = new ClassPathResource(DEFAULT_CONFIG).getInputStream()) {
      defaultConfig = MAPPER.readValue(stream, CustomSearchConfiguration.class);
    }
    try (InputStream stream = new ClassPathResource(TEST_FIXTURE_CONFIG).getInputStream()) {
      fixtureConfig = MAPPER.readValue(stream, CustomSearchConfiguration.class);

      // test specifics
      ((List<Map<String, Object>>)
              fixtureConfig.getQueryConfigurations().get(1).getFunctionScore().get("functions"))
          .remove(1);

      ((List<Map<String, Object>>)
              fixtureConfig.getQueryConfigurations().get(2).getFunctionScore().get("functions"))
          .remove(1);
    }

    assertEquals(fixtureConfig, defaultConfig);
  }
}
