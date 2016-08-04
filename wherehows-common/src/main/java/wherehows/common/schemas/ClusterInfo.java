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
package wherehows.common.schemas;

public class ClusterInfo {
  int clusterId;
  String clusterCode;
  String clusterShortName;
  String datacenterCode;
  String clusterType;
  String deploymentTierCode;

  public ClusterInfo(int clusterId, String clusterCode, String clusterShortName, String datacenterCode,
      String clusterType, String deploymentTierCode) {
    this.clusterId = clusterId;
    this.clusterCode = clusterCode;
    this.clusterShortName = clusterShortName;
    this.datacenterCode = datacenterCode;
    this.clusterType = clusterType;
    this.deploymentTierCode = deploymentTierCode;
  }

  public int getClusterId() {
    return clusterId;
  }

  public String getClusterCode() {
    return clusterCode;
  }

  public String getClusterShortName() {
    return clusterShortName;
  }

  public String getDatacenterCode() {
    return datacenterCode;
  }

  public String getClusterType() {
    return clusterType;
  }

  public String getDeploymentTierCode() {
    return deploymentTierCode;
  }
}
