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

import java.util.List;
import java.util.Map;


public class DatasetConstraintRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  String constraintType;
  String constraintSubType;
  String constraintName;
  String constraintExpression;
  Boolean enabled;
  List<DatasetFieldIndexRecord> referredFields;
  Map<String, String> additionalReferences;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "constraint_type", "constraint_sub_type", "constraint_name",
        "constraint_expression", "enabled", "referred_fields", "additional_reference", "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetConstraintRecord() {
  }

  public Integer getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(Integer datasetId) {
    this.datasetId = datasetId;
  }

  public String getDatasetUrn() {
    return datasetUrn;
  }

  public void setDatasetUrn(String datasetUrn) {
    this.datasetUrn = datasetUrn;
  }

  public String getConstraintType() {
    return constraintType;
  }

  public void setConstraintType(String constraintType) {
    this.constraintType = constraintType;
  }

  public String getConstraintSubType() {
    return constraintSubType;
  }

  public void setConstraintSubType(String constraintSubType) {
    this.constraintSubType = constraintSubType;
  }

  public String getConstraintName() {
    return constraintName;
  }

  public void setConstraintName(String constraintName) {
    this.constraintName = constraintName;
  }

  public String getConstraintExpression() {
    return constraintExpression;
  }

  public void setConstraintExpression(String constraintExpression) {
    this.constraintExpression = constraintExpression;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public List<DatasetFieldIndexRecord> getReferredFields() {
    return referredFields;
  }

  public void setReferredFields(List<DatasetFieldIndexRecord> referredFields) {
    this.referredFields = referredFields;
  }

  public Map<String, String> getAdditionalReferences() {
    return additionalReferences;
  }

  public void setAdditionalReferences(Map<String, String> additionalReferences) {
    this.additionalReferences = this.additionalReferences;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
