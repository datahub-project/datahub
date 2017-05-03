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
package models.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import models.daos.LineageDao;
import org.apache.avro.generic.GenericData;
import play.Logger;
import wherehows.common.schemas.Record;


public class JobExecutionLineageProcessor extends KafkaConsumerProcessor {

  /**
   * Process a JobExecutionLineageEvent record
   * @param record GenericData.Record
   * @param topic String
   * @throws Exception
   * @return null
   */
  public Record process(GenericData.Record record, String topic)
      throws Exception {
    if (record != null) {
      // Logger.info("Processing Job Execution Lineage Event record. ");

      final GenericData.Record auditHeader = (GenericData.Record) record.get("auditHeader");

      final JsonNode rootNode = new ObjectMapper().readTree(record.toString());
      LineageDao.updateJobExecutionLineage(rootNode);
    }
    return null;
  }
}
