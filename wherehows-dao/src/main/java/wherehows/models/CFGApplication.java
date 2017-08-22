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
@Table(name = "cfg_application")
@NoArgsConstructor
@AllArgsConstructor
public class CFGApplication {

  @Id
  @Column(name = "app_id", nullable = false)
  int appId;

  @Column(name = "app_code", nullable = false)
  String appCode;

  @Column(name = "description", nullable = false)
  String description;

  @Column(name = "tech_matrix_id")
  Integer techMatrixId;

  @Column(name = "doc_url")
  String docUrl;

  @Column(name = "parent_app_id", nullable = false)
  Integer parentAppId;

  @Column(name = "app_status", nullable = false)
  String appStatus;

  @Column(name = "last_modified", nullable = false)
  Date lastModified;

  @Column(name = "is_logical")
  String isLogical;

  @Column(name = "uri_type")
  String uriType;

  @Column(name = "uri")
  String uri;

  @Column(name = "lifecycle_layer_id")
  Integer lifecycleLayerId;

  @Column(name = "short_connection_string")
  String shortConnectionString;
}
