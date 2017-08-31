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
package wherehows.models.view;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@Entity
@IdClass(DatasetOwner.DatasetOwnerKey.class)
public class DatasetOwner {

  @Id
  @Column(name = "owner_id")
  private String userName;

  @Id
  @Column(name = "owner_source")
  private String source;

  @Column(name = "namespace")
  private String namespace;

  @Column(name = "display_name")
  private String name;

  @Column(name = "email")
  private String email;

  @Column(name = "is_group")
  private Boolean isGroup;

  @Column(name = "is_active")
  private Boolean isActive;

  @Column(name = "owner_id_type")
  private String idType;

  @Column(name = "owner_type")
  private String type;

  @Column(name = "owner_sub_type")
  private String subType;

  @Column(name = "sort_id")
  private Integer sortId;

  @Column(name = "confirmed_by")
  private String confirmedBy;

  @Column(name = "modified_time")
  private Long modifiedTime;

  static class DatasetOwnerKey implements Serializable {
    private String userName;
    private String source;
  }

  // Copy constructor
  public DatasetOwner(DatasetOwner owner) {
    this.userName = owner.getUserName();
    this.email = owner.getEmail();
    this.name = owner.getName();
    this.namespace = owner.getNamespace();
    this.idType = owner.getIdType();
    this.source = owner.getSource();
    this.type = owner.getType();
    this.subType = owner.getSubType();
    this.isGroup = owner.getIsGroup();
    this.isActive = owner.getIsActive();
    this.sortId = owner.getSortId();
    this.confirmedBy = owner.getConfirmedBy();
    this.modifiedTime = owner.getModifiedTime();
  }
}
