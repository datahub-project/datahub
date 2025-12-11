/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.event;

import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface GenericProducer<T> {
  void setWritable(boolean writable);

  Future<?> send(ProducerRecord<String, T> producerRecord, @Nullable Callback callback);

  void flush();
}
