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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import wherehows.common.Constant;
import wherehows.common.PathAnalyzer;
import wherehows.common.schemas.AzkabanJobExecRecord;
import wherehows.common.writers.DatabaseWriter;


/**
 * Created by zsun on 9/10/15.
 */
public class AzLineageExtractorTest {
  Properties prop;
  String connUrl;
  Connection conn;

  @BeforeTest
  public void setUp()
    throws Exception {
    this.prop = new LineageTest().properties;
    String wherehowsUrl = prop.getProperty(Constant.WH_DB_URL_KEY);
    String wherehowsUserName = prop.getProperty(Constant.WH_DB_USERNAME_KEY);
    String wherehowsPassWord = prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
    connUrl = wherehowsUrl + "?" + "user=" + wherehowsUserName + "&password=" + wherehowsPassWord;
    this.conn = DriverManager.getConnection(connUrl);
    AzLogParser.initialize(conn);
    PathAnalyzer.initialize(conn);
  }

  /**
   * Test extract one job execution's lineage
   * @throws Exception
   */
  @Test(groups = {"needConfig"})
  public void extractLineageTest()
    throws Exception {

    // need to use a real example to test, and this job's hadoop configure file can't be expired
    // AzkabanJobExecRecord aje = new AzkabanJobExecRecord(31, "hadoop-datasets-stats_sizeAggr", (long) 898347, 0, 0, "S",
    //  "WherehowsETL:hadoop-datasets-stats");
    AzkabanJobExecRecord aje = new AzkabanJobExecRecord(31, "endorsements_push-dev-endorsements-suggested-endorsements", (long) 1075483, 0, 0, "S",
      "endorsements:endorsements");
    aje.setJobExecId((long) 1075483037);

    Statement statement = conn.createStatement();
    statement.execute("TRUNCATE TABLE stg_job_execution_data_lineage");
    AzExecMessage message = new AzExecMessage(aje, prop);
    message.databaseWriter = new DatabaseWriter(connUrl, "stg_job_execution_data_lineage");
    message.hnne = new HadoopJobHistoryNodeExtractor(prop);
    message.adc = new AzDbCommunicator(prop);
    message.connection = conn;
    AzLineageExtractor.extract(message);

    ResultSet rs = statement.executeQuery("select count(*) from stg_job_execution_data_lineage");
    rs.next();
    int totalCount = rs.getInt("count(*)");
    int expectedTotalCount = 15;
    Assert.assertEquals(totalCount, expectedTotalCount);
    statement.execute("TRUNCATE TABLE stg_job_execution_data_lineage");
    conn.close();
  }
}
