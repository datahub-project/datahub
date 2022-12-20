package io.datahubproject.openapi.test;

import com.linkedin.mxe.Topics;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTestConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestConsumer.class);

  private CountDownLatch latch = new CountDownLatch(1);

  private String payload;

  @KafkaListener(id = "test-mcp-consumer", topics = Topics.METADATA_CHANGE_PROPOSAL)
  public void receive(ConsumerRecord<?, ?> consumerRecord) {
    LOGGER.info("received payload='{}'", consumerRecord.toString());

    payload = consumerRecord.toString();
    latch.countDown();
  }

  public CountDownLatch getLatch() {
    return latch;
  }

  public void resetLatch() {
    latch = new CountDownLatch(1);
  }

  public String getPayload() {
    return payload;
  }

}