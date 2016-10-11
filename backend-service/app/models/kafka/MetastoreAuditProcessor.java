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

import org.apache.avro.generic.GenericData;
import wherehows.common.schemas.MetastoreAuditRecord;
import wherehows.common.schemas.Record;
import wherehows.common.utils.ClusterUtil;
import wherehows.common.utils.StringUtil;


public class MetastoreAuditProcessor extends KafkaConsumerProcessor {

  /**
   * Process a Metastore Table/Partition Audit event record
   * @param record
   * @param topic
   * @return Record
   * @throws Exception
   */
  @Override
  public Record process(GenericData.Record record, String topic) throws Exception {
    MetastoreAuditRecord eventRecord = null;

    // handle MetastoreTableAuditEvent and MetastorePartitionAuditEvent
    if (record != null) {
      // logger.info("Processing Metastore Audit event record.");

      final GenericData.Record auditHeader = (GenericData.Record) record.get("auditHeader");
      final String server = ClusterUtil.matchClusterCode(String.valueOf(auditHeader.get("server")));
      final String instance = String.valueOf(auditHeader.get("instance"));
      final String appName = String.valueOf(auditHeader.get("appName"));

      String eventName;
      GenericData.Record content;
      final Object oldInfo;
      final Object newInfo;

      // check if it is MetastoreTableAuditEvent
      if (record.get("metastoreTableAuditContent") != null) {
        eventName = "MetastoreTableAuditEvent";
        content = (GenericData.Record) record.get("metastoreTableAuditContent");
        oldInfo = content.get("oldTable");
        newInfo = content.get("newTable");
      }
      // check if it is MetastorePartitionAuditEvent
      else if (record.get("metastorePartitionAuditContent") != null) {
        eventName = "MetastorePartitionAuditEvent";
        content = (GenericData.Record) record.get("metastorePartitionAuditContent");
        oldInfo = content.get("oldPartition");
        newInfo = content.get("newPartition");
      }
      else {
        throw new IllegalArgumentException("Unknown Metastore Audit event: " + record);
      }

      final String eventType = String.valueOf(content.get("eventType"));
      final String metastoreThriftUri = String.valueOf(content.get("metastoreThriftUri"));
      final String metastoreVersion = StringUtil.toStringReplaceNull(content.get("metastoreVersion"), null);
      final long timestamp = (long) content.get("timestamp");
      final String isSuccessful = String.valueOf(content.get("isSuccessful"));
      final String isDataDeleted = String.valueOf(content.get("isDataDeleted"));

      // use newOne, if null, use oldOne
      final GenericData.Record rec = newInfo != null ? (GenericData.Record) newInfo : (GenericData.Record) oldInfo;
      final String dbName = String.valueOf(rec.get("dbName"));
      final String tableName = String.valueOf(rec.get("tableName"));
      // set null / "null" partition to '?' for primary key
      final String partition = StringUtil.toStringReplaceNull(rec.get("values"), "?");
      final String location = StringUtil.toStringReplaceNull(rec.get("location"), null);
      final String owner = StringUtil.toStringReplaceNull(rec.get("owner"), null);
      final long createTime = (long) rec.get("createTime");
      final long lastAccessTime = (long) rec.get("lastAccessTime");

      eventRecord = new MetastoreAuditRecord(server, instance, appName, eventName, eventType, timestamp);
      eventRecord.setEventInfo(metastoreThriftUri, metastoreVersion, isSuccessful, isDataDeleted);
      eventRecord.setTableInfo(dbName, tableName, partition, location, owner, createTime, lastAccessTime);
      // eventRecord.setOldInfo(StringUtil.toStringReplaceNull(oldInfo, null));
      // eventRecord.setNewInfo(StringUtil.toStringReplaceNull(newInfo, null));
    }
    return eventRecord;
  }
}
