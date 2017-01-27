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
import metadata.etl.models.EtlJobName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * For a standalone lineage ETL entrance
 * java -jar metadata-ETL.jar dev
 */
public class Launcher {

  /** job property parameter keys */
  public static final String JOB_NAME_KEY = "job";
  public static final String REF_ID_KEY = "refId";
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
    String etlJobNameString = null;
    int refId = 0;
    long whEtlExecId = 0;
    Properties props = new Properties();

    try (InputStream propFile = new FileInputStream(property_file)) {
      props.load(propFile);
      etlJobNameString = props.getProperty(JOB_NAME_KEY);
      refId = Integer.valueOf(props.getProperty(REF_ID_KEY));
      whEtlExecId = Integer.valueOf(props.getProperty(WH_ETL_EXEC_ID_KEY));

      System.setProperty(LOGGER_CONTEXT_NAME_KEY, etlJobNameString);
    } catch (IOException e) {
       //logger.error("property file '{}' not found" , property_file);
      e.printStackTrace();
      System.exit(1);
    }

    // create the etl job
    EtlJobName etlJobName = EtlJobName.valueOf(etlJobNameString);
    EtlJob etlJob = EtlJobFactory.getEtlJob(etlJobName, refId, whEtlExecId, props);

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
