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

import java.util.Date;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Entity
@Table(name = "dict_field_detail")
@NoArgsConstructor
public class DictFieldDetail {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "field_id", nullable = false)
  private int fieldId;

  @Column(name = "dataset_id", nullable = false)
  private int datasetId;

  @Column(name = "fields_layout_id", nullable = false)
  private int fieldsLayoutId;

  @Column(name = "sort_id", nullable = false)
  private int sortId;

  @Column(name = "parent_sort_id", nullable = false)
  private int parentSortId;

  @Column(name = "parent_path")
  private String parentPath;

  @Column(name = "field_name", nullable = false)
  private String fieldName;

  @Column(name = "field_label")
  private String fieldLabel;

  @Column(name = "data_type", nullable = false)
  private String dataType;

  @Column(name = "data_size")
  private Integer dataSize; // max size 50

  @Column(name = "data_precision")
  private Integer dataPrecision;

  @Column(name = "data_fraction")
  private Integer dataFraction;

  @Column(name = "default_comment_id")
  private Integer defaultCommentId;

  @Column(name = "comment_ids")
  private String commentIds;

  @Column(name = "is_nullable")
  private String isNullable;

  @Column(name = "is_indexed")
  private String isIndexed;

  @Column(name = "is_partitioned")
  private String isPartitioned;

  @Column(name = "is_distributed")
  private Integer isDistributed;

  @Column(name = "default_value")
  private String defaultValue;

  @Column(name = "namespace")
  private String nameSpace;

  @Column(name = "java_data_type")
  private String javaDataType;

  @Column(name = "jdbc_data_type")
  private String jdbcDataType;

  @Column(name = "pig_data_type")
  private String pigDataType;

  @Column(name = "hcatalog_data_type")
  private String hcatalogDataType;

  @Column(name = "modified")
  private Date modified;

  @Column(name = "confidential_flags")
  private String confidentialFlags;

  @Column(name = "is_recursive")
  private String isRecursive;

  @PreUpdate
  @PrePersist
  void prePersist() {
    this.modified = new Date();
  }
}
