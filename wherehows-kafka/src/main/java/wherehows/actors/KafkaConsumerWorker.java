/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.actors;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.Deserializer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import akka.actor.UntypedActor;
import wherehows.common.kafka.schemaregistry.client.SchemaRegistryClient;
import wherehows.common.kafka.serializers.KafkaAvroDeserializer;
import wherehows.common.schemas.AbstractRecord;
import wherehows.common.writers.DatabaseWriter;


/**
 * Akka actor for listening to a Kafka topic and waiting for messages.
 */
@Slf4j
public class KafkaConsumerWorker extends UntypedActor {
  private final String _topic;
  private final int _threadId;
  private final KafkaStream<byte[], byte[]> _kafkaStream;
  private final SchemaRegistryClient _schemaRegistryRestfulClient;
  private final Object _processorClass;
  private final Method _processorMethod;
  private int _receivedRecordCount;

  public KafkaConsumerWorker(String topic, int threadNumber,
      KafkaStream<byte[], byte[]> stream, SchemaRegistryClient schemaRegistryRestfulClient,
      Object processorClass, Method processorMethod, DatabaseWriter dbWriter) {
    this._topic = topic;
    this._threadId = threadNumber;
    this._kafkaStream = stream;
    this._schemaRegistryRestfulClient = schemaRegistryRestfulClient;
    this._processorClass = processorClass;
    this._processorMethod = processorMethod;
    this._receivedRecordCount = 0;  // number of received kafka messages
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message.equals("ApplicationStart")) {
      log.info("Starting Thread: " + _threadId + " for topic: " + _topic);
      final ConsumerIterator<byte[], byte[]> it = _kafkaStream.iterator();
      final Deserializer<Object> avroDeserializer = new KafkaAvroDeserializer(_schemaRegistryRestfulClient);

      while (it.hasNext()) { // block for next input
        _receivedRecordCount++;

        try {
          MessageAndMetadata<byte[], byte[]> msg = it.next();
          GenericData.Record kafkaMsgRecord = (GenericData.Record) avroDeserializer.deserialize(_topic, msg.message());
          // Logger.debug("Kafka worker ThreadId " + _threadId + " Topic " + _topic + " record: " + rec);

          // invoke processor
          final AbstractRecord record = (AbstractRecord) _processorMethod.invoke(
              _processorClass, kafkaMsgRecord, _topic);

        } catch (InvocationTargetException ite) {
          log.error("Processing topic " + _topic + " record error: " + ite.getCause()
              + " - " + ite.getTargetException());
        } catch (Throwable ex) {
          log.error("Error in notify order. ", ex);
        }

        if (_receivedRecordCount % 1000 == 0) {
          log.debug(_topic + " received " + _receivedRecordCount);
        }
      }
      log.info("Shutting down Thread: " + _threadId + " for topic: " + _topic);
    } else {
      unhandled(message);
    }
  }
}
