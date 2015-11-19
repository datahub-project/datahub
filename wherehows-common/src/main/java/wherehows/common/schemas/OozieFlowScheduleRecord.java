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

/**
 * Created by zechen on 10/4/15.
 */
public class OozieFlowScheduleRecord extends AzkabanFlowScheduleRecord {
  public OozieFlowScheduleRecord(Integer appId, String flowPath, String frequency, Integer interval,
    Long effectiveStartTime, Long effectiveEndTime, String refId, Long whExecId) {
    super(appId, flowPath, frequency, interval, effectiveStartTime, effectiveEndTime, refId, whExecId);
  }
}
