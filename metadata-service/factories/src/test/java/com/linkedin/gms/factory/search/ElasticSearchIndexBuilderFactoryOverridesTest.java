package com.linkedin.gms.factory.search;

import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    properties = {
      "elasticsearch.index.settingsOverrides={\"my_index\":{\"number_of_shards\":\"10\"}}",
      "elasticsearch.index.entitySettingsOverrides={\"my_entity\":{\"number_of_shards\":\"5\"}}",
      "elasticsearch.index.prefix=test_prefix"
    },
    classes = {ElasticSearchIndexBuilderFactory.class})
@EnableConfigurationProperties(ConfigurationProvider.class)
public class ElasticSearchIndexBuilderFactoryOverridesTest
    extends AbstractTestNGSpringContextTests {
  @Autowired ESIndexBuilder test;

  @Test
  void testInjection() {
    assertNotNull(test);
    assertEquals(
        "10", test.getIndexSettingOverrides().get("test_prefix_my_index").get("number_of_shards"));
    assertEquals(
        "5",
        test.getIndexSettingOverrides()
            .get("test_prefix_my_entityindex_v2")
            .get("number_of_shards"));
  }
}
