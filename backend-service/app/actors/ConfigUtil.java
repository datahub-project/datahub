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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import metadata.etl.Launcher;
import metadata.etl.models.EtlJobName;
import wherehows.common.Constant;


/**
 * Utility class for generate cmd, write and delete config files.
 */
public class ConfigUtil {

  final static String javaCmd = "java";

  /**
   * Generate the config file in the 'wherehows.app_folder' folder
   * The file name is {whEtlExecId}.config
   * @param etlJobName
   * @param refId
   * @param whEtlExecId
   * @param props
   * @return void
   */
  public static void generateProperties(EtlJobName etlJobName, int refId, long whEtlExecId, Properties props)
      throws IOException {

    props.setProperty(Launcher.JOB_NAME_KEY, etlJobName.name());
    props.setProperty(Launcher.REF_ID_KEY, String.valueOf(refId));
    props.setProperty(Launcher.WH_ETL_EXEC_ID_KEY, String.valueOf(whEtlExecId));

    String dirName = props.getProperty(Constant.WH_APP_FOLDER_KEY) + "/exec";
    File dir = new File(dirName);
    if (!dir.exists()) {
      dir.mkdir();
    }
    File configFile = new File(dirName, whEtlExecId + ".properties");
    FileWriter writer = new FileWriter(configFile);
    props.store(writer, "exec id : " + whEtlExecId + " job configurations");
    writer.close();

  }

  public static void deletePropertiesFile(Properties props, long whEtlExecId) {
    String dirName = props.getProperty(Constant.WH_APP_FOLDER_KEY) + "/exec";
    File configFile = new File(dirName, whEtlExecId + ".properties");
    if (configFile.exists()) {
      configFile.delete();
    }
  }

  public static String generateCMD(long whEtlExecId, String cmdParam) {

    StringBuilder sb = new StringBuilder();
    sb.append(javaCmd);
    sb.append(cmdParam).append(" ");
    String classPath = System.getProperty("java.class.path");
    sb.append("-cp").append(" '").append(classPath).append("' ");
    String dirName = "/var/tmp/wherehows/exec";
    sb.append("-Dconfig=").append(dirName + "/" + whEtlExecId).append(".properties ");
    sb.append("metadata.etl.Launcher");

    return sb.toString();
  }
}
