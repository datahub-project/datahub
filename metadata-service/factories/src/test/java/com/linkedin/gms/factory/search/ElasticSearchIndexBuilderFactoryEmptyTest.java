package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    properties = {
      "elasticsearch.index.settingsOverrides=",
      "elasticsearch.index.entitySettingsOverrides=",
      "elasticsearch.index.prefix=test_prefix"
    },
    classes = {ElasticSearchIndexBuilderFactory.class})
@EnableConfigurationProperties(ConfigurationProvider.class)
public class ElasticSearchIndexBuilderFactoryEmptyTest extends AbstractTestNGSpringContextTests {
  @Autowired ESIndexBuilder test;

  @Test
  void testInjection() {
    assertNotNull(test);
    assertEquals(Map.of(), test.getIndexSettingOverrides());
  }
}
