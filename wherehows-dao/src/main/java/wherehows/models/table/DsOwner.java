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
import javax.persistence.IdClass;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Entity
@Table(name = "dataset_owner")
@NoArgsConstructor
@IdClass(DsOwner.PrimaryKey.class)
public class DsOwner {

  @Id
  @Column(name = "dataset_id", nullable = false)
  private int datasetId;

  @Id
  @Column(name = "owner_id", nullable = false)
  private String ownerId;

  @Id
  @Column(name = "app_id", nullable = false)
  private int appId;

  @Column(name = "namespace", nullable = false)
  private String namespace;

  @Column(name = "owner_type")
  private String ownerType;

  @Column(name = "db_ids")
  private String dbIds;

  @Column(name = "is_group")
  private String isGroup;

  @Column(name = "is_active")
  private String isActive;

  @Column(name = "is_deleted")
  private String isDeleted;

  @Column(name = "sort_id")
  private Integer sortId;

  @Column(name = "source_time")
  private Integer sourceTime;

  @Column(name = "created_time")
  private Integer createdTime;

  @Column(name = "modified_time")
  private Integer modifiedTime;

  @Column(name = "wh_etl_exec_id")
  private Long whEtlExecId;

  @Column(name = "dataset_urn", nullable = false)
  private String datasetUrn;

  @Column(name = "owner_sub_type")
  private String ownerSubType;

  @Column(name = "owner_id_type")
  private String ownerIdType;

  @Id
  @Column(name = "owner_source", nullable = false)
  private String ownerSource;

  @Column(name = "confirmed_by")
  private String confirmedBy;

  @Column(name = "confirmed_on")
  private Integer confirmedOn;

  public static class PrimaryKey implements Serializable {
    private int datasetId;
    private String ownerId;
    private int appId;
    private String ownerSource;
  }

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
