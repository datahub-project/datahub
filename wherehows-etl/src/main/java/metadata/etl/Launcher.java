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
package metadata.etl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import metadata.etl.models.EtlJobFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;


/**
 * For a standalone lineage ETL entrance
 * java -jar wherehows-etl.jar dev
 */
public class Launcher {

  /** job property parameter keys */
  public static final String WH_ETL_EXEC_ID_KEY = "whEtlId";
  public static final String LOGGER_CONTEXT_NAME_KEY = "CONTEXT_NAME";

  /** command line config file location parameter key */
  private static final String CONFIG_FILE_LOCATION_KEY = "config";

  protected static final Logger logger = LoggerFactory.getLogger("Job Launcher");

  /**
   * Read config file location from command line. Read all configuration from command line, execute the job.
   * Example command line : java -Dconfig=/path/to/config/file -cp "lib/*" metadata.etl.Launcher
   * @param args contain the config file location parameter 'confg'
   * @throws Exception
   */
  public static void main(String[] args)
      throws Exception {

    String property_file = System.getProperty(CONFIG_FILE_LOCATION_KEY, null);
    String etlClassName = null;
    long whEtlExecId = 0;
    Properties props = new Properties();

    try (InputStream propFile = new FileInputStream(property_file)) {
      props.load(propFile);
      etlClassName = props.getProperty(Constant.JOB_CLASS_KEY);
      whEtlExecId = Integer.valueOf(props.getProperty(WH_ETL_EXEC_ID_KEY));

      System.setProperty(LOGGER_CONTEXT_NAME_KEY, etlClassName);
    } catch (IOException e) {
       //logger.error("property file '{}' not found" , property_file);
      e.printStackTrace();
      System.exit(1);
    }

    if (etlClassName == null) {
      logger.error("Must specify {} in properties file", Constant.JOB_CLASS_KEY);
      System.exit(1);
    }

    // create the etl job
    EtlJob etlJob = null;
    try {
      etlJob = EtlJobFactory.getEtlJob(etlClassName, whEtlExecId, props);
    } catch (Exception e) {
      logger.error("Failed to create ETL job {}: {}", etlClassName, e.getMessage());
      System.exit(1);
    }

    try {
      etlJob.run();
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      e.printStackTrace(new PrintWriter(sw));
      String errorString = sw.toString();
      logger.error(errorString);
      if (errorString.contains("IndexError") || errorString.contains("ImportError")) {
        System.exit(2);
      }
      System.exit(1);
    }

    logger.info("whEtlExecId=" + whEtlExecId + " finished.");
    System.exit(0);
  }

}
