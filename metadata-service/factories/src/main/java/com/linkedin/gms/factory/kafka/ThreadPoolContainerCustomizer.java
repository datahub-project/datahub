package com.linkedin.gms.factory.kafka;

import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class ThreadPoolContainerCustomizer
    implements ContainerCustomizer<
        String, GenericRecord, ConcurrentMessageListenerContainer<String, GenericRecord>> {
  @Override
  public void configure(ConcurrentMessageListenerContainer<String, GenericRecord> container) {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    // Default Queue Capacity is set to max, so we want to allow the thread pool to add concurrent
    // threads up to configured value
    threadPoolTaskExecutor.setCorePoolSize(container.getConcurrency());
    threadPoolTaskExecutor.setMaxPoolSize(container.getConcurrency());
    threadPoolTaskExecutor.initialize();
    container.getContainerProperties().setConsumerTaskExecutor(threadPoolTaskExecutor);
  }
}
