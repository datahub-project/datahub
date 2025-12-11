/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
