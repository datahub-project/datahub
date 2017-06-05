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
package msgs;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Properties;
import metadata.etl.models.EtlType;
import metadata.etl.models.RefIdType;
import wherehows.common.Constant;


/**
 * Created by zechen on 9/4/15.
 */
public class EtlJobMessage {
  private static int DEFAULT_TIMEOUT = 20000;

  private Long whEtlExecId;
  private String etlJobName;
  private Properties etlJobProperties;

  public EtlJobMessage(String etlJobName, Properties etlJobProperties) {
    this.etlJobName = etlJobName;
    this.etlJobProperties = etlJobProperties;
  }

  public Long getWhEtlExecId() {
    return whEtlExecId;
  }

  public void setWhEtlExecId(Long whEtlExecId) {
    this.whEtlExecId = whEtlExecId;
  }

  public String getEtlJobName() {
    return etlJobName;
  }

  public Properties getEtlJobProperties() {
    return etlJobProperties;
  }

  public String getCmdParam() {
    return etlJobProperties.getProperty(Constant.JOB_CMD_PARAMS_KEY, "");
  }

  public int getTimeout() {
    String timeout = etlJobProperties.getProperty(Constant.JOB_TIMEOUT_KEY, null);
    if (timeout == null) {
      return DEFAULT_TIMEOUT;
    }
    return Integer.parseInt(timeout);
  }

  public String getCronExpr() {
    return etlJobProperties.getProperty(Constant.JOB_CRON_EXPR_KEY, null);
  }

  /**
   * For debuging
   * @return
   */
  public String toDebugString() {
    return String.format("(jobName:%s, whEtlExecId:%d)", this.etlJobName, this.whEtlExecId);
  }
}
