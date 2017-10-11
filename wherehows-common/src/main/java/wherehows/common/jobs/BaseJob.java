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
package wherehows.common.jobs;

import java.util.Properties;
import wherehows.common.Constant;


public abstract class BaseJob {

  public final Properties prop;

  public BaseJob(long whExecId, Properties properties) {
    this.prop = properties;
    this.prop.setProperty(Constant.WH_EXEC_ID_KEY, String.valueOf(whExecId));
  }

  public abstract void run() throws Exception;
}
