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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import wherehows.common.DatasetPath;
import wherehows.common.LineageCombiner;
import wherehows.common.PathAnalyzer;
import wherehows.common.schemas.LineageRecord;


/**
 * Created by zsun on 9/13/15.
 */
@Test(groups = {"wherehows.common"})
public class LineageCombinerTest {
  final static String TEST_PROP_FILE_NAME = "wherehows-common-test.properties";
  LineageCombiner lineageCombiner;
  Properties testProp;

  @BeforeTest
  public void setUp()
      throws SQLException, IOException {
    InputStream inputStream = this.getClass().getResourceAsStream(TEST_PROP_FILE_NAME);
    testProp = new Properties();
    if (inputStream != null) {
      testProp.load(inputStream);
    } else {
      throw new FileNotFoundException("Lack of configuration file for testing: " + TEST_PROP_FILE_NAME);
    }
    String wherehowsHost = testProp.getProperty("wherehows.db.jdbc.url");
    String wherehowsUserName = testProp.getProperty("wherehows.db.username");
    String wherehowsPassWord = testProp.getProperty("wherehows.db.password");
    Connection conn = DriverManager
        .getConnection(wherehowsHost + "?" + "user=" + wherehowsUserName + "&password=" + wherehowsPassWord);

    lineageCombiner = new LineageCombiner(conn);
    PathAnalyzer.initialize(conn);
  }

  @Test(groups = {"needConfig"})
  public void lineageCombinerTest() {
    int appId = Integer.valueOf(testProp.getProperty("lineageRecord.appId"));
    long flowExecId = Long.valueOf(testProp.getProperty("lineageRecord.flowExecId"));
    String jobName = testProp.getProperty(testProp.getProperty("lineageRecord.jobName"));
    long jobExecId = Long.valueOf(testProp.getProperty("lineageRecord.jobExecId"));

    LineageRecord lineageRecord1 = new LineageRecord(appId, flowExecId, jobName, jobExecId);
    int databaseId1 = Integer.valueOf(testProp.getProperty("lineageRecord1.databaseId"));
    String fullObjectName1 = testProp.getProperty("lineageRecord1.fullObjectName");
    String storageType1 = testProp.getProperty("ineageRecord1.storageType");
    String sourceTargetType1 = testProp.getProperty("lineageRecord1.sourceTargetType");
    String operation1 = testProp.getProperty("lineageRecord1.operation");
    String someFlowPath1 = testProp.getProperty("lineageRecord1.flowPath");

    lineageRecord1.setDatasetInfo(databaseId1, fullObjectName1, storageType1);
    lineageRecord1.setOperationInfo(sourceTargetType1, operation1, null, null, null, null, 0, 0, someFlowPath1);

    LineageRecord lineageRecord2 = new LineageRecord(appId, flowExecId, jobName, jobExecId);
    int databaseId2 = Integer.valueOf(testProp.getProperty("lineageRecord2.databaseId"));
    String fullObjectName2 = testProp.getProperty("lineageRecord2.fullObjectName");
    String storageType2 = testProp.getProperty("ineageRecord2.storageType");
    String sourceTargetType2 = testProp.getProperty("lineageRecord2.sourceTargetType");
    String operation2 = testProp.getProperty("lineageRecord2.operation");
    String someFlowPath2 = testProp.getProperty("lineageRecord2.flowPath");

    lineageRecord2.setDatasetInfo(databaseId2, fullObjectName2, storageType2);
    lineageRecord2.setOperationInfo(sourceTargetType2, operation2, null, null, null, null, 0, 0, someFlowPath2);

    LineageRecord lineageRecord3 = new LineageRecord(appId, flowExecId, jobName, jobExecId);
    int databaseId3 = Integer.valueOf(testProp.getProperty("lineageRecord3.databaseId"));
    String fullObjectName3 = testProp.getProperty("lineageRecord3.fullObjectName");
    String storageType3 = testProp.getProperty("ineageRecord3.storageType");
    String sourceTargetType3 = testProp.getProperty("lineageRecord3.sourceTargetType");
    String operation3 = testProp.getProperty("lineageRecord3.operation");
    String someFlowPath3 = testProp.getProperty("lineageRecord3.flowPath");

    lineageRecord3.setDatasetInfo(databaseId3, fullObjectName3, storageType3);
    lineageRecord3.setOperationInfo(sourceTargetType3, operation3, null, null, null, null, 0, 0, someFlowPath3);

    List<LineageRecord> allLineage = new ArrayList<>();
    allLineage.add(lineageRecord1);
    allLineage.add(lineageRecord2);
    allLineage.add(lineageRecord3);

    lineageCombiner.addAll(allLineage);

    System.out.println(lineageCombiner.getCombinedLineage().get(0).toDatabaseValue().substring(0, 264));

    // the second last column is time stamp, so use sub string to ignore the rest
    Assert.assertEquals(lineageCombiner.getCombinedLineage().get(0).toDatabaseValue().substring(0, 264),
        testProp.getProperty("lineageCombinerDatabaseValue").substring(0, 264));
  }

  /**
   * Need the file path pattern info in dataset_partition_layout_pattern table.
   */
  @Test(groups = {"needConfig"})
  public void analyzeTest() {
    String fullPath = testProp.getProperty("analyzeTest.fullPath");
    DatasetPath datasetPath = PathAnalyzer.analyze(fullPath);
    Assert.assertEquals(datasetPath.abstractPath, testProp.getProperty("analyzeTest.abstractPath"));
  }
}