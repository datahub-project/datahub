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

import com.fasterxml.jackson.annotation.JsonIgnore;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Transient;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@Entity
public class DatasetColumn {

  @Id
  @Column(name = "field_id")
  private Long id;

  @Column(name = "sort_id")
  private int sortID;

  @Column(name = "parent_sort_id")
  private int parentSortID;

  @Column(name = "field_name")
  private String fieldName;

  @JsonIgnore
  @Column(name = "parent_path")
  private String parentPath;

  @Transient
  private String fullFieldPath;

  @Column(name = "data_type")
  private String dataType;

  @Column(name = "comment")
  private String comment;

  @Column(name = "comment_count")
  private Long commentCount;

  @JsonIgnore
  @Column(name = "partitioned")
  private String partitionedStr;

  @Transient
  private boolean partitioned;

  @JsonIgnore
  @Column(name = "nullable")
  private String nullableStr;

  @Transient
  private boolean nullable;

  @JsonIgnore
  @Column(name = "indexed")
  private String indexedStr;

  @Transient
  private boolean indexed;

  @JsonIgnore
  @Column(name = "distributed")
  private String distributedStr;

  @Transient
  private boolean distributed;

  @Transient
  private String treeGridClass;
}
