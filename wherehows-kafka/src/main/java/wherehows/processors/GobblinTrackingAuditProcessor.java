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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import wherehows.service.GobblinTrackingAuditService;


@Slf4j
@RequiredArgsConstructor
public class GobblinTrackingAuditProcessor extends KafkaConsumerProcessor {

  private static final String DALI_LIMITED_RETENTION_AUDITOR = "DaliLimitedRetentionAuditor";
  private static final String DALI_AUTOPURGED_AUDITOR = "DaliAutoPurgeAuditor";
  private static final String DS_IGNORE_IDPC_AUDITOR = "DsIgnoreIDPCAuditor";
  private static final String METADATA_FILE_CLASSIFIER = "MetadataFileClassifier";

  private final GobblinTrackingAuditService gobblinTrackingAuditService;

  /**
   * Process a Gobblin tracking event audit record
   * @param record
   * @param topic
   * @return null
   * @throws Exception
   */
  public void process(GenericData.Record record, String topic) throws Exception {

    if (record == null || record.get("name") == null) {
      return;
    }

    final String name = record.get("name").toString();
    // only handle "DaliLimitedRetentionAuditor","DaliAutoPurgeAuditor" and "DsIgnoreIDPCAuditor"
    if (name.equals(DALI_LIMITED_RETENTION_AUDITOR) || name.equals(DALI_AUTOPURGED_AUDITOR) || name.equals(
        DS_IGNORE_IDPC_AUDITOR)) {
      // TODO: Re-enable this once it's fixed.
    } else if (name.equals(METADATA_FILE_CLASSIFIER)) {
      gobblinTrackingAuditService.updateHdfsDatasetSchema(record);
    }
  }



}
