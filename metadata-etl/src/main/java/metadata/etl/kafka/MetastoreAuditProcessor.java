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
package metadata.etl.kafka;

import org.apache.avro.generic.GenericData;
import wherehows.common.schemas.MetastoreAuditRecord;
import wherehows.common.schemas.Record;


public class MetastoreAuditProcessor extends KafkaConsumerProcessor {

  /**
   * Process a Metastore Table/Partition Audit event record
   * @param record
   * @param topic
   * @throws Exception
   */
  @Override
  public Record process(GenericData.Record record, String topic) throws Exception {
    MetastoreAuditRecord eventRecord = null;

    // handle MetastoreTableAuditEvent and MetastorePartitionAuditEvent
    if (record != null) {
      logger.info("Processing Metastore Audit event record.");

      final GenericData.Record auditHeader = (GenericData.Record) record.get("auditHeader");
      final String server = utf8ToString(auditHeader.get("server"));
      final String instance = utf8ToString(auditHeader.get("instance"));
      final String appName = utf8ToString(auditHeader.get("appName"));

      String eventName;
      GenericData.Record content;
      final Object oldOne;
      final Object newOne;

      // check if it is MetastoreTableAuditEvent
      if (record.get("metastoreTableAuditContent") != null) {
        eventName = "MetastoreTableAuditEvent";
        content = (GenericData.Record) record.get("metastoreTableAuditContent");
        oldOne = content.get("oldTable");
        newOne = content.get("newTable");
      }
      // check if it is MetastorePartitionAuditEvent
      else if (record.get("metastorePartitionAuditContent") != null) {
        eventName = "MetastorePartitionAuditEvent";
        content = (GenericData.Record) record.get("metastorePartitionAuditContent");
        oldOne = content.get("oldPartition");
        newOne = content.get("newPartition");
      }
      else {
        throw new IllegalArgumentException("Unknown Metastore Audit event: " + record);
      }

      final String eventType = utf8ToString(content.get("eventType"));
      final String metastoreThriftUri = utf8ToString(content.get("metastoreThriftUri"));
      final String metastoreVersion = utf8ToString(content.get("metastoreVersion"));
      final long timestamp = (long) content.get("timestamp");
      final String isSuccessful = utf8ToString(content.get("isSuccessful"));
      final String isDataDeleted = utf8ToString(content.get("isDataDeleted"));

      // use newOne, if null, use oldOne
      final GenericData.Record rec = newOne != null ? (GenericData.Record) newOne : (GenericData.Record) oldOne;
      final String dbName = utf8ToString(rec.get("dbName"));
      final String tableName = utf8ToString(rec.get("tableName"));
      final String partition = utf8ToString(rec.get("values"));
      final String location = utf8ToString(rec.get("location"));
      final String owner = utf8ToString(rec.get("owner"));
      final long createTime = (long) rec.get("createTime");
      final long lastAccessTime = (long) rec.get("lastAccessTime");

      eventRecord = new MetastoreAuditRecord(server, instance, appName, eventName, eventType, timestamp);
      eventRecord.setEventInfo(metastoreThriftUri, metastoreVersion, isSuccessful, isDataDeleted);
      // set null partition to '?' for primary key
      eventRecord.setTableInfo(dbName, tableName, (partition != null ? partition : "?"),
          location, owner, createTime, lastAccessTime);
      eventRecord.setOldOne(utf8ToString(oldOne));
      eventRecord.setNewOne(utf8ToString(newOne));
    }
    return eventRecord;
  }

  /**
   * Cast utf8 text to String, also handle null
   * @param text utf8
   * @return String
   */
  private String utf8ToString(Object text) {
    return text == null ? null : text.toString();
  }
}
