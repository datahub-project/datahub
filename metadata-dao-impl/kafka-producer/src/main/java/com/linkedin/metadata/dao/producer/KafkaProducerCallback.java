package com.linkedin.metadata.dao.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class KafkaProducerCallback implements Callback {
  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    log.info("Kafka producer callback:");
    log.info("  Metadata: " + (metadata == null ? "null" : metadata.toString()));
    log.info("  Exception: " + (exception == null ? "null" : exception.toString()));
  }
}
