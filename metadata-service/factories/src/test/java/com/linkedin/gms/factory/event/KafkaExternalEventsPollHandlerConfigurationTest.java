package com.linkedin.gms.factory.event;

import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datahubproject.event.ExternalEventsPollHandler;
import io.datahubproject.event.KafkaExternalEventsPollHandler;
import io.datahubproject.event.kafka.KafkaConsumerPool;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class KafkaExternalEventsPollHandlerConfigurationTest {

  @Test
  public void kafkaExternalEventsPollHandlerBean() {
    KafkaExternalEventsPollHandlerConfiguration configuration =
        new KafkaExternalEventsPollHandlerConfiguration();
    ExternalEventsPollHandler handler =
        configuration.kafkaExternalEventsPollHandler(
            Mockito.mock(KafkaConsumerPool.class), new ObjectMapper());
    assertTrue(handler instanceof KafkaExternalEventsPollHandler);
  }
}
