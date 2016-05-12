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
package metadata.etl.lineage;

import java.sql.Connection;
import java.util.Properties;
import wherehows.common.schemas.AzkabanJobExecRecord;
import wherehows.common.writers.DatabaseWriter;


/**
 * Created by zsun on 9/16/15.
 */
public class AzExecMessage {
  public AzkabanJobExecRecord azkabanJobExecution;
  public Properties prop;

  public AzServiceCommunicator asc;
  public HadoopJobHistoryNodeExtractor hnne;
  public AzDbCommunicator adc;
  public DatabaseWriter databaseWriter;
  public Connection connection;

  public AzExecMessage(AzkabanJobExecRecord azkabanJobExecution, Properties prop) {
    this.azkabanJobExecution = azkabanJobExecution;
    this.prop = prop;
  }

  public String toString() {
    return azkabanJobExecution.toString();
  }
}
