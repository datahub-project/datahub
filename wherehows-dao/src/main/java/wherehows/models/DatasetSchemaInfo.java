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
package wherehows.models;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Entity
@Table(name = "dataset_classification")
@NoArgsConstructor
@AllArgsConstructor
public class DatasetSchemaInfo {

  @Id
  @Column(name = "dataset_id", nullable = false)
  int datasetId;

  @Column(name = "dataset_urn", nullable = false)
  String datasetUrn;

  @Column(name = "is_backward_compatible")
  Boolean isBackwardCompatible;

  @Column(name = "create_time", nullable = false)
  int createTime;

  @Column(name = "revision")
  Integer revision;

  @Column(name = "version")
  String version;

  @Column(name = "name")
  String name;

  @Column(name = "description")
  String description;

  @Column(name = "format")
  String format;

  @Column(name = "original_schema")
  String originalSchema;

  @Column(name = "original_schema_checksum")
  String originalSchemaChecksum;

  @Column(name = "key_schema_type")
  String keySchemaType;

  @Column(name = "key_schema_format")
  String keySchemaFormat;

  @Column(name = "key_schema")
  String keySchema;

  @Column(name = "is_field_name_case_sensitive")
  boolean isFieldNameCaseSensitive;

  @Column(name = "field_schema")
  String fieldSchema;

  @Column(name = "change_data_capture_fields")
  String changeDataCaptureFields;

  @Column(name = "audit_fields")
  String auditFields;

  @Column(name = "modified_time")
  Integer modifiedTime;
}
