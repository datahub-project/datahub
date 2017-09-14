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
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Entity
@Table(name = "dict_dataset")
@AllArgsConstructor
@NoArgsConstructor
public class DictDataset {

  @Id
  int id;

  @Column(name = "name", nullable = false)
  String name;

  @Column(name = "schema")
  String schema;

  @Column(name = "schema_type")
  String schemaType;

  @Column(name = "properties")
  String properties;

  @Column(name = "fields")
  String fields;

  @Column(name = "urn")
  String urn;

  @Column(name = "source")
  String source;

  @Column(name = "location_prefix")
  String locationPrefix;

  @Column(name = "parent_name")
  String parentName;

  @Column(name = "storage_type")
  String storageType;

  @Column(name = "ref_dataset_id")
  Integer refDatasetId;

  @Column(name = "dataset_type")
  String datasetType;

  @Column(name = "is_active")
  Boolean isActive;

  @Column(name = "is_deprecated")
  Boolean isDeprecated;

  @Column(name = "hive_serdes_class")
  String hiveSerdesClass;

  @Column(name = "is_partitioned")
  String isPartitioned;

  @Column(name = "partition_layout_pattern_id")
  Integer partitionLayoutPatternId;

  @Column(name = "sample_partition_full_path")
  String samplePartitionFullPath;

  @Column(name = "source_created_time")
  Integer sourceCreatedTime;

  @Column(name = "source_modified_time")
  Integer sourceModifiedTime;

  @Column(name = "created_time")
  Integer createdTime;

  @Column(name = "modified_time")
  Integer modifiedTime;

  @Column(name = "wh_etl_exec_id")
  Integer etlExecId;

  @PreUpdate
  @PrePersist
  void prePersist() {
    Integer timestamp = Integer.valueOf((int)(System.currentTimeMillis() / 1000));
    this.modifiedTime = timestamp;
    if (this.createdTime == null) {
      this.createdTime = timestamp;
    }
  }
}
