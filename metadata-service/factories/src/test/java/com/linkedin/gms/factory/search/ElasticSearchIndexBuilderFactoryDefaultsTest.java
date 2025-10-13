package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.Map;
import org.mockito.Answers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@TestPropertySource(locations = "classpath:/application.yaml")
@SpringBootTest(classes = {ElasticSearchIndexBuilderFactory.class})
@EnableConfigurationProperties(ConfigurationProvider.class)
public class ElasticSearchIndexBuilderFactoryDefaultsTest extends AbstractTestNGSpringContextTests {
  @Autowired ESIndexBuilder test;

  @MockitoBean(name = "searchClientShim", answers = Answers.RETURNS_MOCKS)
  SearchClientShim<?> searchClientShim;

  @Test
  void testInjection() {
    assertNotNull(test);
    assertEquals(Map.of(), test.getIndexSettingOverrides());
  }
}
