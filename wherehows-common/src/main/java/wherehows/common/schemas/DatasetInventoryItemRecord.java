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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;


public class DatasetInventoryItemRecord extends AbstractRecord {

  String nativeName;
  String dataOrigin;
  DatasetInventoryPropertiesRecord datasetProperties;
  String dataPlatformUrn;
  String eventDate;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"native_name", "data_origin", "dataset_properties", "data_platform", "event_date"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  @JsonIgnore
  public static String[] getInventoryItemColumns() {
    return new String[]{"native_name", "data_origin", "data_platform", "event_date", "change_actor_urn", "change_type",
        "change_time", "change_note", "native_type", "uri", "dataset_name_case_sensitivity",
        "field_name_case_sensitivity", "data_content_case_sensitivity"};
  }

  @JsonIgnore
  public Object[] getInventoryItemValues() {
    if (datasetProperties == null) {
      return new Object[]{nativeName, dataOrigin, dataPlatformUrn, eventDate, null, null, null, null, null, null, null,
          null, null};
    } else {
      DatasetChangeAuditStamp changes = datasetProperties.getChangeAuditStamp();
      DatasetCaseSensitiveRecord cases = datasetProperties.getCaseSensitivity();
      return new Object[]{nativeName, dataOrigin, dataPlatformUrn, eventDate, changes.getActorUrn(), changes.getType(),
          changes.getTime(), changes.getNote(), datasetProperties.getNativeType(), datasetProperties.getUri(),
          cases.getDatasetName(), cases.getFieldName(), cases.getDataContent()};
    }
  }

  public DatasetInventoryItemRecord() {
  }

  public String getNativeName() {
    return nativeName;
  }

  public void setNativeName(String nativeName) {
    this.nativeName = nativeName;
  }

  public String getDataOrigin() {
    return dataOrigin;
  }

  public void setDataOrigin(String dataOrigin) {
    this.dataOrigin = dataOrigin;
  }

  public DatasetInventoryPropertiesRecord getDatasetProperties() {
    return datasetProperties;
  }

  public void setDatasetProperties(DatasetInventoryPropertiesRecord datasetProperties) {
    this.datasetProperties = datasetProperties;
  }

  public String getDataPlatformUrn() {
    return dataPlatformUrn;
  }

  public void setDataPlatformUrn(String dataPlatformUrn) {
    this.dataPlatformUrn = dataPlatformUrn;
  }

  public String getEventDate() {
    return eventDate;
  }

  public void setEventDate(String eventDate) {
    this.eventDate = eventDate;
  }
}
