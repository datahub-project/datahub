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

  /** command line parameter keys */
  public static final String JOB_NAME_KEY = "job";
  public static final String REF_ID_KEY = "refId";
  public static final String WH_ETL_EXEC_ID_KEY = "whEtlId";

  /** Only for test */
  private static final String CONFIG_FILE_LOCATION_KEY = "config";

  protected static final Logger logger = LoggerFactory.getLogger("Job Launcher");

  /**
   * It can run as a standalone application
   * @param args
   * @throws Exception
   */
  public static void main(String[] args)
      throws Exception {
    String etlJobNameString = System.getProperty(JOB_NAME_KEY);
    String property_file = System.getProperty(CONFIG_FILE_LOCATION_KEY, null);
    Properties props = new Properties();
    int refId = Integer.valueOf(System.getProperty(REF_ID_KEY, "0"));
    long whEtlId = Integer.valueOf(System.getProperty(WH_ETL_EXEC_ID_KEY, "0"));



    if (property_file != null) {  // test mode
      try (InputStream propFile = new FileInputStream(property_file)) {
        props.load(propFile);
      } catch (IOException e) {
        //logger.error("property file '{}' not found" , property_file);
        e.printStackTrace();
      }
    }
    else { // production mode
      Properties properties = System.getProperties();
      props.putAll(properties);
    }

    // create the etl job
    EtlJobName etlJobName = EtlJobName.valueOf(etlJobNameString);
    EtlJob etlJob = EtlJobFactory.getEtlJob(etlJobName, refId, whEtlId, props);

    try {
      etlJob.run();
    } catch (Exception e) {

      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      logger.error(sw.toString());
      System.exit(1);
    }

    logger.info("whEtlId: " + whEtlId + " now stop");
    System.exit(0);

  }

}
