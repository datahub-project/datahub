package com.linkedin.metadata.kafka;

import static org.testng.AssertJUnit.*;

import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.service.FormService;
import io.datahubproject.metadata.jobs.common.health.kafka.KafkaHealthIndicator;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {MaeConsumerApplication.class, MaeConsumerApplicationTestConfiguration.class})
public class MaeConsumerApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired private EntityService<?> mockEntityService;

  @Autowired private KafkaHealthIndicator kafkaHealthIndicator;

  @Autowired private FormService formService;

  @Autowired private EntitySearchService entitySearchService;

  @Test
  public void testMaeConsumerAutoWiring() {
    assertNotNull(mockEntityService);
    assertNotNull(kafkaHealthIndicator);
    assertNotNull(formService);
  }

  @Test
  public void testPostConstruct() throws IllegalAccessException {
    ESSearchDAO test = (ESSearchDAO) FieldUtils.readField(entitySearchService, "esSearchDAO", true);
    AspectRetriever aspectRetriever =
        (AspectRetriever) FieldUtils.readField(test, "aspectRetriever", true);
    assertNotNull(aspectRetriever);
  }
}
