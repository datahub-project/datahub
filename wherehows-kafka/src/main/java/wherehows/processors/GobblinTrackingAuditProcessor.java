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

import gobblin.metrics.GobblinTrackingEvent_audit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import wherehows.dao.DaoFactory;
import wherehows.service.GobblinTrackingAuditService;


@Slf4j
public class GobblinTrackingAuditProcessor extends KafkaMessageProcessor {

  private static final String DALI_LIMITED_RETENTION_AUDITOR = "DaliLimitedRetentionAuditor";
  private static final String DALI_AUTOPURGED_AUDITOR = "DaliAutoPurgeAuditor";
  private static final String DS_IGNORE_IDPC_AUDITOR = "DsIgnoreIDPCAuditor";
  private static final String METADATA_FILE_CLASSIFIER = "MetadataFileClassifier";

  private final GobblinTrackingAuditService gobblinTrackingAuditService;

  public GobblinTrackingAuditProcessor(DaoFactory daoFactory, KafkaProducer<String, IndexedRecord> producer) {
    super(daoFactory, producer);
    gobblinTrackingAuditService =
        new GobblinTrackingAuditService(DAO_FACTORY.getDatasetClassificationDao(), DAO_FACTORY.getDictDatasetDao());
  }

  /**
   * Process a Gobblin tracking event audit record
   * @param indexedRecord
   * @throws Exception
   */
  public void process(IndexedRecord indexedRecord) throws Exception {

    if (indexedRecord == null || indexedRecord.getClass() != GobblinTrackingEvent_audit.class) {
      log.debug("Event record type error");
      return;
    }

    GobblinTrackingEvent_audit record = (GobblinTrackingEvent_audit) indexedRecord;

    String name = String.valueOf(record.name);
    // only handle "DaliLimitedRetentionAuditor","DaliAutoPurgeAuditor" and "DsIgnoreIDPCAuditor"
    if (name.equals(DALI_LIMITED_RETENTION_AUDITOR) || name.equals(DALI_AUTOPURGED_AUDITOR) || name.equals(
        DS_IGNORE_IDPC_AUDITOR)) {
      // TODO: Re-enable this once it's fixed.
    } else if (name.equals(METADATA_FILE_CLASSIFIER)) {
      gobblinTrackingAuditService.updateHdfsDatasetSchema(record);
    } else {
      log.debug("Gobblin audit message skipped.");
    }
  }
}
