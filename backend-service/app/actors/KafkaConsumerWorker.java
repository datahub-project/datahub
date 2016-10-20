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
package actors;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.Deserializer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import akka.actor.UntypedActor;
import msgs.KafkaCommMsg;
import play.Logger;
import wherehows.common.kafka.schemaregistry.client.SchemaRegistryClient;
import wherehows.common.kafka.serializers.KafkaAvroDeserializer;
import wherehows.common.schemas.AbstractRecord;
import wherehows.common.writers.DatabaseWriter;


/**
 * Akka actor for listening to a Kafka topic and waiting for messages.
 */
public class KafkaConsumerWorker extends UntypedActor {
  private final String _topic;
  private final int _threadId;
  private final KafkaStream<byte[], byte[]> _kafkaStream;
  private final SchemaRegistryClient _schemaRegistryRestfulClient;
  private final Object _processorClass;
  private final Method _processorMethod;
  private final DatabaseWriter _dbWriter;
  private int _receivedRecordCount;
  private int _processedRecordCount;

  public KafkaConsumerWorker(String topic, int threadNumber,
      KafkaStream<byte[], byte[]> stream, SchemaRegistryClient schemaRegistryRestfulClient,
      Object processorClass, Method processorMethod, DatabaseWriter dbWriter) {
    this._topic = topic;
    this._threadId = threadNumber;
    this._kafkaStream = stream;
    this._schemaRegistryRestfulClient = schemaRegistryRestfulClient;
    this._processorClass = processorClass;
    this._processorMethod = processorMethod;
    this._dbWriter = dbWriter;
    this._receivedRecordCount = 0;  // number of received kafka messages
    this._processedRecordCount = 0; // number of processed records
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message.equals("Start")) {
      Logger.info("Starting Thread: " + _threadId + " for topic: " + _topic);
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

          // save record to database
          if (record != null) {
            _dbWriter.append(record);
            // _dbWriter.close();
            _dbWriter.insert();
            _processedRecordCount++;
          }
        } catch (InvocationTargetException ite) {
          Logger.error("Processing topic " + _topic + " record error: " + ite.getCause()
              + " - " + ite.getTargetException());
        } catch (SQLException | IOException e) {
          Logger.error("Error while inserting event record: ", e);
        } catch (Throwable ex) {
          Logger.error("Error in notify order. ", ex);
        }

        if (_receivedRecordCount % 1000 == 0) {
          Logger.debug(_topic + " received " + _receivedRecordCount + " processed " + _processedRecordCount);
        }
      }
      Logger.info("Shutting down Thread: " + _threadId + " for topic: " + _topic);
    } else {
      unhandled(message);
    }
  }
}
