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
package actors;

import java.util.Enumeration;
import java.util.Properties;
import metadata.etl.Launcher;
import metadata.etl.models.EtlJobName;


/**
 * Utility class for command line
 */
public class CmdUtil {
  /**
   * Generate a command that start a ETL job.
   * @param etlJobName
   * @param refId
   * @param whEtlExecId
   * @param props
   * @return command
   */
  final static String javaCmd = "java ";
  public static String generateCMD(EtlJobName etlJobName, int refId, long whEtlExecId, Properties props, String cmdParam) {
    StringBuilder sb = new StringBuilder();
    sb.append(javaCmd);
    sb.append(cmdParam).append(" ");
    sb.append("-D").append(Launcher.JOB_NAME_KEY).append("=").append(etlJobName).append(" ");
    sb.append("-D").append(Launcher.REF_ID_KEY).append("=").append(refId).append(" ");
    sb.append("-D").append(Launcher.WH_ETL_EXEC_ID_KEY).append("=").append(whEtlExecId).append(" ");

    Enumeration e = props.propertyNames();
    while (e.hasMoreElements()) {
      String key = (String)e.nextElement();
      String value = props.getProperty(key);
      sb.append("-D").append(key).append("=").append(value).append(" ");
    }

    String classPath = System.getProperty("java.class.path");
    sb.append("-cp").append(" '").append(classPath).append("' ");
    sb.append("metadata.etl.Launcher");

    return sb.toString();
  }
}
