/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.search;

import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import org.mockito.Answers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
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

  @MockitoBean(name = "searchClientShim", answers = Answers.RETURNS_MOCKS)
  SearchClientShim<?> searchClientShim;

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
