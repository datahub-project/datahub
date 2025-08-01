package com.linkedin.metadata.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@SpringBootTest(classes = DataHubTestApplication.class)
public class DataHubAppConfigurationTest extends AbstractTestNGSpringContextTests {

  @Autowired private DataHubTestApplication testApplication;

  @Test
  public void testInit() {
    assertNotNull(testApplication);
  }

  @Test
  public void testMCPBatchDefaults() {
    assertFalse(
        testApplication
            .getDataHubAppConfig()
            .getMetadataChangeProposal()
            .getConsumer()
            .getBatch()
            .isEnabled());
    assertEquals(
        testApplication
            .getDataHubAppConfig()
            .getMetadataChangeProposal()
            .getConsumer()
            .getBatch()
            .getSize(),
        15744000);
  }

  @Test
  public void testCustomSearchConfiguration() throws IOException {
    assertNotNull(testApplication.getDataHubAppConfig());
    assertNotNull(testApplication.getDataHubAppConfig().getElasticSearch());
    assertNotNull(testApplication.getDataHubAppConfig().getElasticSearch().getSearch());
    assertNotNull(testApplication.getDataHubAppConfig().getElasticSearch().getSearch().getCustom());
    assertNotNull(
        testApplication
            .getDataHubAppConfig()
            .getElasticSearch()
            .getSearch()
            .getCustom()
            .resolve(new YAMLMapper()));
  }
}
