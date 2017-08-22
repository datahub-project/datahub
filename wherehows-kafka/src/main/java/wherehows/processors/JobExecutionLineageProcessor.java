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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import wherehows.service.JobExecutionLineageService;


@Slf4j
@RequiredArgsConstructor
public class JobExecutionLineageProcessor extends KafkaConsumerProcessor {

  private final JobExecutionLineageService jobExecutionLineageService;
  /**
   * Process a JobExecutionLineageEvent record
   * @param record GenericData.Record
   * @param topic String
   * @throws Exception
   * @return null
   */
  public void process(GenericData.Record record, String topic)
      throws Exception {
    if (record != null) {
      // Logger.info("Processing Job Execution Lineage Event record. ");

      final GenericData.Record auditHeader = (GenericData.Record) record.get("auditHeader");

      final JsonNode rootNode = new ObjectMapper().readTree(record.toString());
      jobExecutionLineageService.updateJobExecutionLineage(rootNode);
    }
  }
}
