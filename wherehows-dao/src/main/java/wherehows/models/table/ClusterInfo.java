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
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Entity
@Table(name = "cfg_cluster")
@NoArgsConstructor
@AllArgsConstructor
public class ClusterInfo {

  @Id
  @Column(name = "cluster_id")
  int clusterId;

  @Column(name = "cluster_code")
  String clusterCode;

  @Column(name = "cluster_short_name")
  String clusterShortName;

  @Column(name = "cluster_type")
  String clusterType;

  @Column(name = "deployment_tier_code")
  String deploymentTierCode;

  @Column(name = "data_center_code")
  String datacenterCode;

  @Column(name = "last_modified")
  Date lastModified;

  @Column(name = "description")
  String description;
}
