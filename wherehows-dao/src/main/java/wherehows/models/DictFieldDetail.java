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

import java.util.Date;
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
public class DictFieldDetail {

  @Id
  @Column(name = "field_id", nullable = false)
  int fieldId;

  @Column(name = "dataset_id", nullable = false)
  int datasetId;

  @Column(name = "fields_layout_id", nullable = false)
  int fieldsLayoutId;

  @Column(name = "sort_id", nullable = false)
  int sortId;

  @Column(name = "parent_sort_id", nullable = false)
  int parentSortId;

  @Column(name = "parent_path")
  String parentPath;

  @Column(name = "field_name", nullable = false)
  String fieldName;

  @Column(name = "field_label")
  String fieldLabel;

  @Column(name = "data_type", nullable = false)
  String dataType;

  @Column(name = "data_size")
  int dataSize;

  @Column(name = "data_precision")
  int dataPrecision;

  @Column(name = "data_fraction")
  int dataFraction;

  @Column(name = "default_comment_id")
  int defaultCommentId;

  @Column(name = "comment_ids")
  String commentIds;

  @Column(name = "is_nullable")
  String isNullable;

  @Column(name = "is_indexed")
  String isIndexed;

  @Column(name = "is_partitioned")
  String isPartitioned;

  @Column(name = "is_distributed")
  int isDistributed;

  @Column(name = "default_value")
  String defaultValue;

  @Column(name = "namespace")
  String nameSpace;

  @Column(name = "java_data_type")
  String javaDataType;

  @Column(name = "jdbc_data_type")
  String jdbcDataType;

  @Column(name = "pig_data_type")
  String pigDataType;

  @Column(name = "hcatalog_data_type")
  String hcatalogDataType;

  @Column(name = "modified")
  Date modified;

  @Column(name = "confidential_flags")
  int confidentialFlags;

  @Column(name = "is_recursive")
  int isRecursive;
}
