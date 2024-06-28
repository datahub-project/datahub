package com.linkedin.metadata.kafka;

import static org.testng.AssertJUnit.*;

import com.datahub.event.PlatformEventProcessor;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.service.FormService;
import io.datahubproject.metadata.jobs.common.health.kafka.KafkaHealthIndicator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    classes = {MaeConsumerApplication.class, MaeConsumerApplicationTestConfiguration.class},
    properties = "PE_CONSUMER_ENABLED=true")
public class MaeConsumerApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired private EntityService<?> mockEntityService;

  @Autowired private KafkaHealthIndicator kafkaHealthIndicator;

  @Autowired private FormService formService;

  @Autowired private EntitySearchService entitySearchService;

  @Autowired private PlatformEventProcessor platformEventProcessor;

  @Test
  public void testMaeConsumerAutoWiring() {
    assertNotNull(mockEntityService);
    assertNotNull(kafkaHealthIndicator);
    assertNotNull(formService);
    assertNotNull(platformEventProcessor);
  }
}
