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

import java.util.List;
import java.util.Map;


public class DeploymentRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  String deploymentTier;
  String dataCenter;
  String region;
  String zone;
  String cluster;
  String container;
  Boolean enabled;
  Map<String, String> additionalDeploymentInfo;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "deployment_tier", "datacenter", "region", "zone", "cluster",
        "container", "enabled", "additional_info", "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DeploymentRecord() {
  }

  public Integer getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(Integer datasetId) {
    this.datasetId = datasetId;
  }

  public String getDatasetUrn() {
    return datasetUrn;
  }

  public void setDatasetUrn(String datasetUrn) {
    this.datasetUrn = datasetUrn;
  }

  public String getDeploymentTier() {
    return deploymentTier;
  }

  public void setDeploymentTier(String deploymentTier) {
    this.deploymentTier = deploymentTier;
  }

  public String getDataCenter() {
    return dataCenter;
  }

  public void setDataCenter(String dataCenter) {
    this.dataCenter = dataCenter;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getZone() {
    return zone;
  }

  public void setZone(String zone) {
    this.zone = zone;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getContainer() {
    return container;
  }

  public void setContainer(String container) {
    this.container = container;
  }

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public Map<String, String> getAdditionalDeploymentInfo() {
    return additionalDeploymentInfo;
  }

  public void setAdditionalDeploymentInfo(Map<String, String> additionalDeploymentInfo) {
    this.additionalDeploymentInfo = additionalDeploymentInfo;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
