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
