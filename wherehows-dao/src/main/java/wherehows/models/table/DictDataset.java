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
@Table(name = "dict_dataset")
@NoArgsConstructor
public class DictDataset {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private int id;

  @Column(name = "\"name\"", nullable = false)
  private String name;

  @Column(name = "\"schema\"")
  private String schema;

  @Column(name = "schema_type")
  private String schemaType;

  @Column(name = "properties")
  private String properties;

  @Column(name = "\"fields\"")
  private String fields;

  @Column(name = "urn", nullable = false)
  private String urn;

  @Column(name = "\"source\"")
  private String source;

  @Column(name = "location_prefix")
  private String locationPrefix;

  @Column(name = "parent_name")
  private String parentName;

  @Column(name = "storage_type")
  private String storageType;

  @Column(name = "ref_dataset_id")
  private Integer refDatasetId;

  @Column(name = "dataset_type")
  private String datasetType;

  @Column(name = "is_active")
  private Boolean isActive;

  @Column(name = "is_deprecated")
  private Boolean isDeprecated;

  @Column(name = "hive_serdes_class")
  private String hiveSerdesClass;

  @Column(name = "is_partitioned")
  private String isPartitioned;

  @Column(name = "partition_layout_pattern_id")
  private Integer partitionLayoutPatternId;

  @Column(name = "sample_partition_full_path")
  private String samplePartitionFullPath;

  @Column(name = "source_created_time")
  private Integer sourceCreatedTime;

  @Column(name = "source_modified_time")
  private Integer sourceModifiedTime;

  @Column(name = "created_time")
  private Integer createdTime;

  @Column(name = "modified_time")
  private Integer modifiedTime;

  @Column(name = "wh_etl_exec_id")
  private Long etlExecId;

  @PreUpdate
  @PrePersist
  void prePersist() {
    Integer timestamp = (int) (System.currentTimeMillis() / 1000);
    this.modifiedTime = timestamp;
    if (this.createdTime == null) {
      this.createdTime = timestamp;
    }
  }
}
