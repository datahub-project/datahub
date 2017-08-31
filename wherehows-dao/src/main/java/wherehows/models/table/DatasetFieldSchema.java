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
package wherehows.models.table;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Entity
@Table(name = "dataset_field_schema")
@NoArgsConstructor
@AllArgsConstructor
public class DatasetFieldSchema implements Serializable {

  @Id
  @Column(name = "dataset_id", nullable = false)
  int datasetId;

  @Id
  @Column(name = "position", nullable = false)
  int position;

  @Column(name = "parent_field_position", nullable = false)
  int parentFieldPosition;

  @Column(name = "field_json_path")
  String fieldJsonPath;

  @Column(name = "field_path", nullable = false)
  String fieldPath;

  @Column(name = "aliases")
  String aliases;

  @Column(name = "type")
  String type;

  @Column(name = "logical_type")
  String logicalType;

  @Column(name = "semantic_type")
  String semanticType;

  @Column(name = "abstract_type")
  String abstractType;

  @Column(name = "description")
  String description;

  @Column(name = "nullable", nullable = false)
  boolean nullable;

  @Column(name = "default_value")
  String defaultValue;

  @Column(name = "max_byte_length")
  Integer maxByteLength;

  @Column(name = "max_char_length")
  Integer maxCharLength;

  @Column(name = "char_type")
  String charType;

  @Column(name = "precision")
  Integer precision;

  @Column(name = "scale")
  Integer scale;

  @Column(name = "confidential_flags")
  Integer confidentialFlags;

  @Column(name = "is_recursive")
  Integer isRecursive;
}
