package com.linkedin.metadata.config;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(
    classes = DataHubTestApplication.class,
    properties = {
      "ELASTICSEARCH_ENABLED=false",
      "ELASTICSEARCH_ENTITY_INDEX_V3_ENABLED=true",
    })
public class ElasticSearchDisabledEntityIndexOverridesTest
    extends AbstractTestNGSpringContextTests {

  @Autowired private DataHubTestApplication testApplication;

  @Test
  public void elasticsearchDisabledForcesEntityIndexV3Disabled() {
    DataHubAppConfiguration app = testApplication.getDataHubAppConfig();
    assertFalse(app.getElasticSearch().isEnabled());
    assertTrue(app.getElasticSearch().getEntityIndex().getV3().isEnabled());
    assertFalse(app.getElasticSearch().isEffectiveEntityIndexV3Enabled());
    assertFalse(
        app.getElasticSearch().effectiveEntityIndex().getV3().isEnabled(),
        "effective view masks raw v3 when elasticsearch integration is off");
  }
}
