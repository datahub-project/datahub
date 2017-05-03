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

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import metadata.etl.Launcher;
import metadata.etl.models.EtlJobName;
import wherehows.common.Constant;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.*;


/**
 * Utility class for generate cmd, write and delete config files.
 */
class ConfigUtil {

  private static final String javaCmd = "java";
  private static final String WH_APPLICATION_DEFAULT_DIRECTORY = "/var/tmp/wherehows";

  /**
   * Generate the config file in the 'wherehows.app_folder' folder
   * The file name is {whEtlExecId}.config
   *
   * @param etlJobName
   * @param refId
   * @param whEtlExecId
   * @param props
   * @return void
   */
  static void generateProperties(EtlJobName etlJobName, int refId, long whEtlExecId, Properties props)
          throws IOException {

    props.setProperty(Launcher.JOB_NAME_KEY, etlJobName.name());
    props.setProperty(Launcher.REF_ID_KEY, String.valueOf(refId));
    props.setProperty(Launcher.WH_ETL_EXEC_ID_KEY, String.valueOf(whEtlExecId));

    String dirName = props.getProperty(Constant.WH_APP_FOLDER_KEY, WH_APPLICATION_DEFAULT_DIRECTORY) + "/exec";
    File dir = new File(dirName);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    File configFile = new File(dirName, whEtlExecId + ".properties");
    FileWriter writer = new FileWriter(configFile);
    props.store(writer, "exec id : " + whEtlExecId + " job configurations");
    writer.close();
  }

  static void deletePropertiesFile(Properties props, long whEtlExecId) {
    String dirName = props.getProperty(Constant.WH_APP_FOLDER_KEY, WH_APPLICATION_DEFAULT_DIRECTORY) + "/exec";
    File configFile = new File(dirName, whEtlExecId + ".properties");
    if (configFile.exists()) {
      configFile.delete();
    }
  }

  static ProcessBuilder buildProcess(EtlJobName etlJobName, long whEtlExecId, String cmdParam,
      Properties etlJobProperties) {
    String classPath = System.getProperty("java.class.path");
    String outDir = etlJobProperties.getProperty(Constant.WH_APP_FOLDER_KEY, WH_APPLICATION_DEFAULT_DIRECTORY);
    String configFile = outDir + "/exec/" + whEtlExecId + ".properties";

    String[] cmdParams = isNotBlank(cmdParam) ? cmdParam.trim().split(" ") : new String[0];

    ProcessBuilder pb = new ProcessBuilder(new ImmutableList.Builder<String>().add(javaCmd)
        .addAll(Arrays.asList(cmdParams))
        .add("-cp").add(classPath)
        .add("-Dconfig=" + configFile)
        .add("-DCONTEXT=" + etlJobName.name())
        .add("-Dlogback.configurationFile=etl_logback.xml")
        .add("-DLOG_DIR=" + outDir)
        .add("metadata.etl.Launcher")
        .build());
    pb.redirectOutput(ProcessBuilder.Redirect.to(new File(outDir + "/" + etlJobName.name() + ".stdout")));
    pb.redirectError(ProcessBuilder.Redirect.to(new File(outDir + "/" + etlJobName.name() + ".stderr")));
    return pb;
  }
}
