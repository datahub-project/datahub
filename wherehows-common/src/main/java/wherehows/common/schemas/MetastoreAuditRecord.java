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
package wherehows.common.schemas;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Data record model for Metastore Table/Partition Audit events
 *
 */
public class MetastoreAuditRecord extends AbstractRecord {

  String server;
  String instance;
  String appName;
  String eventName;
  String eventType;
  long timestamp;
  String metastoreThriftUri;
  String metastoreVersion;
  String isSuccessful;
  String isDataDeleted;
  String dbName;
  String tableName;
  String partition;
  String location;
  String owner;
  long createTime;
  long lastAccessTime;
  String oldInfo = null;
  String newInfo = null;

  @Override
  public String[] getDbColumnNames() {
    final String[] columnNames = {"server", "instance", "app_name", "event_name", "event_type",
        "log_event_time", "metastore_thrift_uri", "metastore_version", "is_successful", "is_data_deleted",
        "db_name", "table_name", "time_partition", "location", "owner",
        "create_time", "last_access_time", "old_info", "new_info"};
    return columnNames;
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public MetastoreAuditRecord() {
  }

  public MetastoreAuditRecord(String server, String instance, String appName,
      String eventName, String eventType, long timestamp) {
    this.server = server;
    this.instance = instance;
    this.appName = appName;
    this.eventName = eventName;
    this.eventType = eventType;
    this.timestamp = timestamp;
  }

  public void setEventInfo(String metastoreThriftUri, String metastoreVersion,
      String isSuccessful, String isDataDeleted) {
    this.metastoreThriftUri = metastoreThriftUri;
    this.metastoreVersion = metastoreVersion;
    this.isSuccessful = isSuccessful;
    this.isDataDeleted = isDataDeleted;
  }

  public void setTableInfo(String dbName, String tableName, String partition, String location,
      String owner, long createTime, long lastAccessTime) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.partition = partition;
    this.location = location;
    this.owner = owner;
    this.createTime = createTime;
    this.lastAccessTime = lastAccessTime;
  }

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public String getInstance() {
    return instance;
  }

  public void setInstance(String instance) {
    this.instance = instance;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getEventName() {
    return eventName;
  }

  public void setEventName(String eventName) {
    this.eventName = eventName;
  }

  public String getEventType() {
    return eventType;
  }

  public void setEventType(String eventType) {
    this.eventType = eventType;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getMetastoreThriftUri() {
    return metastoreThriftUri;
  }

  public void setMetastoreThriftUri(String metastoreThriftUri) {
    this.metastoreThriftUri = metastoreThriftUri;
  }

  public String getMetastoreVersion() {
    return metastoreVersion;
  }

  public void setMetastoreVersion(String metastoreVersion) {
    this.metastoreVersion = metastoreVersion;
  }

  public String getIsSuccessful() {
    return isSuccessful;
  }

  public void setIsSuccessful(String isSuccessful) {
    this.isSuccessful = isSuccessful;
  }

  public String getIsDataDeleted() {
    return isDataDeleted;
  }

  public void setIsDataDeleted(String isDataDeleted) {
    this.isDataDeleted = isDataDeleted;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getPartition() {
    return partition;
  }

  public void setPartition(String partition) {
    this.partition = partition;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public void setLastAccessTime(long lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }

  public String getOldInfo() {
    return oldInfo;
  }

  public void setOldInfo(String oldInfo) {
    this.oldInfo = oldInfo;
  }

  public String getNewInfo() {
    return newInfo;
  }

  public void setNewInfo(String newInfo) {
    this.newInfo = newInfo;
  }
}
