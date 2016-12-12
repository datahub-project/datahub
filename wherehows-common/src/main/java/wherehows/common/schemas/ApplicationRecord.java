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


public class ApplicationRecord extends AbstractRecord {

  String type;
  String name;
  DeploymentRecord deploymentDetail; // not using the dataset info in DeploymentRecord
  String uri;

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public ApplicationRecord() {
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DeploymentRecord getDeploymentDetail() {
    return deploymentDetail;
  }

  public void setDeploymentDetail(DeploymentRecord deploymentDetail) {
    this.deploymentDetail = deploymentDetail;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }
}
