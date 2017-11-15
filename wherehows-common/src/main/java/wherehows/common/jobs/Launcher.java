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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import wherehows.common.Constant;


/**
 * For a standalone lineage ETL entrance
 * java -jar wherehows-etl.jar dev
 */
@Slf4j
public class Launcher {

  /** job property parameter keys */
  public static final String WH_ETL_EXEC_ID_KEY = "whEtlId";
  public static final String LOGGER_CONTEXT_NAME_KEY = "CONTEXT_NAME";

  /** command line config file location parameter key */
  private static final String CONFIG_FILE_LOCATION_KEY = "config";



  /**
   * Read config file location from command line. Read all configuration from command line, execute the job.
   * Example command line : java -Dconfig=/path/to/config/file -cp "lib/*" wherehows.common.jobs.Launcher
   * @param args contain the config file location parameter 'confg'
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    String propertyFile = System.getProperty(CONFIG_FILE_LOCATION_KEY, null);
    String jobClassName = null;
    long whEtlExecId = 0;
    Properties props = new Properties();

    try (InputStream propFile = new FileInputStream(propertyFile)) {
      props.load(propFile);
      jobClassName = props.getProperty(Constant.JOB_CLASS_KEY);
      if (jobClassName == null) {
        log.error("Must specify {} in properties file", Constant.JOB_CLASS_KEY);
        System.exit(1);
      }

      whEtlExecId = Integer.parseInt(props.getProperty(WH_ETL_EXEC_ID_KEY));

      System.setProperty(LOGGER_CONTEXT_NAME_KEY, jobClassName);
    } catch (IOException e) {
      //logger.error("property file '{}' not found" , property_file);
      e.printStackTrace();
      System.exit(1);
    }

    // create the etl job
    BaseJob job = null;
    try {
      job = JobFactory.getJob(jobClassName, whEtlExecId, props);
    } catch (Exception e) {
      log.error("Failed to create ETL job {}: {}", jobClassName, e.getMessage());
      System.exit(1);
    }

    try {
      job.run();
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String errorString = sw.toString();
      log.error(errorString);
      if (errorString.contains("IndexError") || errorString.contains("ImportError")) {
        System.exit(2);
      }
      System.exit(1);
    }

    log.info("whEtlExecId=" + whEtlExecId + " finished.");
    System.exit(0);
  }
}
