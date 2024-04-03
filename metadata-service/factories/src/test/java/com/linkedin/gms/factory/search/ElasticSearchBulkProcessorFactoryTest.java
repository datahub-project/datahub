package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import org.opensearch.action.support.WriteRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@TestPropertySource(locations = "classpath:/application.yml")
@SpringBootTest(classes = {ElasticSearchBulkProcessorFactory.class})
@EnableConfigurationProperties(ConfigurationProvider.class)
public class ElasticSearchBulkProcessorFactoryTest extends AbstractTestNGSpringContextTests {
  @Autowired ESBulkProcessor test;

  @Test
  void testInjection() {
    assertNotNull(test);
    assertEquals(WriteRequest.RefreshPolicy.NONE, test.getWriteRequestRefreshPolicy());
  }
}
