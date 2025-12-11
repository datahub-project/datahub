/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.event;

import io.datahubproject.event.kafka.KafkaConsumerPool;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@ConditionalOnProperty(name = "eventsApi.enabled", havingValue = "true")
@Configuration
public class KafkaConsumerPoolFactory {
  @Value("${kafka.consumerPool.initialSize:5}")
  private int initialPoolSize;

  @Value("${kafka.consumerPool.maxSize:10}")
  private int maxPoolSize;

  @Bean
  public KafkaConsumerPool kafkaConsumerPool(
      @Qualifier("kafkaConsumerPoolConsumerFactory")
          DefaultKafkaConsumerFactory<String, GenericRecord> consumerFactory) {
    return new KafkaConsumerPool(consumerFactory, initialPoolSize, maxPoolSize);
  }
}
