package com.linkedin.gms.factory.kafka;

import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


public class ThreadPoolContainerCustomizer
    implements ContainerCustomizer<String, GenericRecord, ConcurrentMessageListenerContainer<String, GenericRecord>> {
  @Override
  public void configure(ConcurrentMessageListenerContainer<String, GenericRecord> container) {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    // Min pool size defaults to 1 so we just need to limit the concurrency by the configured value
    threadPoolTaskExecutor.setMaxPoolSize(container.getConcurrency());
    container.getContainerProperties().setConsumerTaskExecutor(threadPoolTaskExecutor);
  }
}
