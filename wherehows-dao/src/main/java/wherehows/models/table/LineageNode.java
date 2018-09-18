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

import java.util.List;


public class LineageNode {

  public int id;
  public String nodeType;
  public String abstractedPath;
  public String storageType;
  public String urn;
  public String jobType;
  public String cluster;
  public String jobPath;
  public String jobName;
  public String scriptName;
  public String scriptPath;
  public String scriptType;
  public String jobStartTime;
  public String jobEndTime;
  public Long jobStartUnixTime;
  public Long jobEndUnixTime;
  public int level;
  public String gitLocation;
  public List<String> sortList;
  public String sourceTargetType;
  public Long execId;
  public Long jobId;
  public Long recordCount;
  public int applicationId;
  public String partitionType;
  public String operation;
  public String partitionStart;
  public String partitionEnd;
  public String fullObjectName;
  public String preJobs;
  public String postJobs;
}
