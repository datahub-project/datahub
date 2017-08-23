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
package wherehows.processors;

import org.apache.avro.generic.GenericData;


public class MetastoreAuditProcessor extends KafkaConsumerProcessor {

  /**
   * Process a Metastore Table/Partition Audit event record
   * @param record
   * @param topic
   * @return Record
   * @throws Exception
   */
  @Override
  public void process(GenericData.Record record, String topic) throws Exception {
  }
}
