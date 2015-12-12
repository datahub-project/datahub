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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;
import wherehows.common.schemas.AzkabanJobExecRecord;


/**
 * Created by zsun on 9/9/15.
 */
public class AzJobCheckerTest {
  final String jsonInput = "{\"attempt\": 0,\n" + " \"endTime\": 1442233069062,\n" + " \"executionId\": 832765,\n"
    + " \"executionOptions\": {\"concurrentOption\": \"skip\",\n" + "                       \"disabled\": [],\n"
    + "                       \"failureAction\": \"FINISH_CURRENTLY_RUNNING\",\n"
    + "                       \"failureEmails\": [\"zsun@linkedin.com\"],\n"
    + "                       \"failureEmailsOverride\": true,\n" + "                       \"flowParameters\": {},\n"
    + "                       \"mailCreator\": \"default\",\n" + "                       \"memoryCheck\": true,\n"
    + "                       \"notifyOnFirstFailure\": false,\n"
    + "                       \"notifyOnLastFailure\": false,\n" + "                       \"pipelineExecId\": null,\n"
    + "                       \"pipelineLevel\": null,\n" + "                       \"queueLevel\": 0,\n"
    + "                       \"successEmails\": [],\n" + "                       \"successEmailsOverride\": false},\n"
    + " \"executionPath\": \"executions/832765\",\n" + " \"flowId\": \"hadoop-datasets-stats\",\n" + " \"id\": null,\n"
    + " \"lastModfiedTime\": 1440783373310,\n" + " \"lastModifiedUser\": \"zsun\",\n"
    + " \"nodes\": [{\"attempt\": 0,\n" + "             \"endTime\": 1442233069053,\n"
    + "             \"id\": \"hadoop-datasets-stats\",\n"
    + "             \"inNodes\": [\"hadoop-datasets-stats_enrich-abstract-dataset\"],\n"
    + "             \"jobSource\": \"hadoop-datasets-stats.job\",\n"
    + "             \"propSource\": \"common.properties\",\n" + "             \"startTime\": 1442233069045,\n"
    + "             \"status\": \"SUCCEEDED\",\n" + "             \"type\": \"noop\",\n"
    + "             \"updateTime\": 1442233069057},\n" + "            {\"attempt\": 0,\n"
    + "             \"endTime\": 1442232636665,\n"
    + "             \"id\": \"hadoop-datasets-stats_load-size-avro-into-mysql\",\n"
    + "             \"inNodes\": [\"hadoop-datasets-stats_sizePartition\"],\n"
    + "             \"jobSource\": \"hadoop-datasets-stats_load-size-avro-into-mysql.job\",\n"
    + "             \"outNodes\": [\"hadoop-datasets-stats_extra-load-size-into-wherehows-mysql\"],\n"
    + "             \"propSource\": \"common.properties\",\n" + "             \"startTime\": 1442229210581,\n"
    + "             \"status\": \"SUCCEEDED\",\n" + "             \"type\": \"hadoopJava\",\n"
    + "             \"updateTime\": 1442232636670},\n" + "            {\"attempt\": 0,\n"
    + "             \"endTime\": 1442229861221,\n"
    + "             \"id\": \"hadoop-datasets-stats_extract-dataset-layout\",\n"
    + "             \"inNodes\": [\"hadoop-datasets-stats_sizePartition\"],\n"
    + "             \"jobSource\": \"hadoop-datasets-stats_extract-dataset-layout.job\",\n"
    + "             \"outNodes\": [\"hadoop-datasets-stats_load-abstract-dataset-into-wherehows-mysql\"],\n"
    + "             \"propSource\": \"common.properties\",\n" + "             \"startTime\": 1442229210582,\n"
    + "             \"status\": \"SUCCEEDED\",\n" + "             \"type\": \"hadoopJava\",\n"
    + "             \"updateTime\": 1442229861231},\n" + "            {\"attempt\": 0,\n"
    + "             \"endTime\": 1442229210579,\n" + "             \"id\": \"hadoop-datasets-stats_sizePartition\",\n"
    + "             \"inNodes\": [\"hadoop-datasets-stats_sizeAggr\"],\n"
    + "             \"jobSource\": \"hadoop-datasets-stats_sizePartition.job\",\n"
    + "             \"outNodes\": [\"hadoop-datasets-stats_load-size-avro-into-mysql\",\n"
    + "                           \"hadoop-datasets-stats_extract-dataset-layout\"],\n"
    + "             \"propSource\": \"common.properties\",\n" + "             \"startTime\": 1442228463681,\n"
    + "             \"status\": \"SUCCEEDED\",\n" + "             \"type\": \"hadoopJava\",\n"
    + "             \"updateTime\": 1442229210587},\n" + "            {\"attempt\": 0,\n"
    + "             \"endTime\": 1442228463629,\n" + "             \"id\": \"hadoop-datasets-stats_sizeAggr\",\n"
    + "             \"jobSource\": \"hadoop-datasets-stats_sizeAggr.job\",\n"
    + "             \"outNodes\": [\"hadoop-datasets-stats_sizePartition\"],\n"
    + "             \"propSource\": \"common.properties\",\n" + "             \"startTime\": 1442224810817,\n"
    + "             \"status\": \"SUCCEEDED\",\n" + "             \"type\": \"hadoopJava\",\n"
    + "             \"updateTime\": 1442228463679},\n" + "            {\"attempt\": 0,\n"
    + "             \"endTime\": 1442229882257,\n"
    + "             \"id\": \"hadoop-datasets-stats_load-abstract-dataset-into-wherehows-mysql\",\n"
    + "             \"inNodes\": [\"hadoop-datasets-stats_extract-dataset-layout\"],\n"
    + "             \"jobSource\": \"hadoop-datasets-stats_load-abstract-dataset-into-wherehows-mysql.job\",\n"
    + "             \"outNodes\": [\"hadoop-datasets-stats_enrich-abstract-dataset\"],\n"
    + "             \"propSource\": \"common.properties\",\n" + "             \"startTime\": 1442229861224,\n"
    + "             \"status\": \"SUCCEEDED\",\n" + "             \"type\": \"hadoopJava\",\n"
    + "             \"updateTime\": 1442229882261},\n" + "            {\"attempt\": 0,\n"
    + "             \"endTime\": 1442233066192,\n"
    + "             \"id\": \"hadoop-datasets-stats_extra-load-size-into-wherehows-mysql\",\n"
    + "             \"inNodes\": [\"hadoop-datasets-stats_load-size-avro-into-mysql\"],\n"
    + "             \"jobSource\": \"hadoop-datasets-stats_extra-load-size-into-wherehows-mysql.job\",\n"
    + "             \"outNodes\": [\"hadoop-datasets-stats_enrich-abstract-dataset\"],\n"
    + "             \"propSource\": \"common.properties\",\n" + "             \"startTime\": 1442232636668,\n"
    + "             \"status\": \"SUCCEEDED\",\n" + "             \"type\": \"hadoopJava\",\n"
    + "             \"updateTime\": 1442233066196},\n" + "            {\"attempt\": 0,\n"
    + "             \"endTime\": 1442233069043,\n"
    + "             \"id\": \"hadoop-datasets-stats_enrich-abstract-dataset\",\n"
    + "             \"inNodes\": [\"hadoop-datasets-stats_load-abstract-dataset-into-wherehows-mysql\",\n"
    + "                          \"hadoop-datasets-stats_extra-load-size-into-wherehows-mysql\"],\n"
    + "             \"jobSource\": \"hadoop-datasets-stats_enrich-abstract-dataset.job\",\n"
    + "             \"outNodes\": [\"hadoop-datasets-stats\"],\n"
    + "             \"propSource\": \"common.properties\",\n" + "             \"startTime\": 1442233066194,\n"
    + "             \"status\": \"SUCCEEDED\",\n" + "             \"type\": \"hadoopJava\",\n"
    + "             \"updateTime\": 1442233069046}],\n" + " \"projectId\": 533,\n"
    + " \"projectName\": \"WherehowsETL\",\n" + " \"properties\": [{\"source\": \"common.properties\"}],\n"
    + " \"proxyUsers\": [\"azkaban@GRID.LINKEDIN.COM\",\n" + "                 \"dev_svc\",\n"
    + "                 \"azkaban/eat1-nertzaz01.grid.linkedin.com@GRID.LINKEDIN.COM\",\n"
    + "                 \"zsun\",\n" + "                 \"data_svc\"],\n" + " \"startTime\": 1442224810815,\n"
    + " \"status\": \"SUCCEEDED\",\n" + " \"submitTime\": 1442224810778,\n" + " \"submitUser\": \"zsun\",\n"
    + " \"type\": null,\n" + " \"updateTime\": 1442233069065,\n" + " \"version\": 301}";


  AzJobChecker ajc;
  Properties prop;

  @BeforeTest
  public void setUp()
    throws SQLException {
    this.prop = new LineageTest().properties;
    ajc = new AzJobChecker(prop);
  }

  @Test(groups = {"needConfig"})
  public void getRecentFinishedJobFromFlowTest()
    throws SQLException, IOException {
    List<AzkabanJobExecRecord> results = ajc.getRecentFinishedJobFromFlow();
    for (AzkabanJobExecRecord a : results) {
      System.out.print(a.getFlowExecId() + "\t");
      System.out.print(a.getJobName() + "\t");
      System.out.println(a.getJobExecId());
    }
    Assert.assertNotNull(results);
  }

  @Test(groups = {"needConfig"})
  public void getRecentFinishedJobFromFlowTest2()
      throws SQLException, IOException {
    List<AzkabanJobExecRecord> results = ajc.getRecentFinishedJobFromFlow(2, 1448916456L);
    for (AzkabanJobExecRecord a : results) {
      System.out.print(a.getFlowExecId() + "\t");
      System.out.print(a.getJobName() + "\t");
      System.out.println(a.getJobExecId());
    }
    Assert.assertNotNull(results);
  }

  @Test(groups = {"needConfig"})
  public void parseJsonTest()
    throws IOException {
    List<AzkabanJobExecRecord> result = ajc.parseJson(jsonInput, 11111);
    for (int i = 0; i < result.size(); i++) {
      AzkabanJobExecRecord aje = result.get(i);
      System.out.println(aje.getJobExecId());
      System.out.println(aje.getJobName());
      System.out.println(aje.getStartTime());
      System.out.println(aje.getEndTime());
      System.out.println(aje.getFlowPath());
      System.out.println();
      Assert.assertEquals((long) aje.getJobExecId(), 11111 * 1000 + i);
    }
  }

  @Test(groups = {"needConfig"})
  public void parseNestedJsonTest()
      throws IOException, URISyntaxException {

    URL url = Thread.currentThread().getContextClassLoader().getResource("nestedJson");
    byte[] encoded = Files.readAllBytes(Paths.get(url.getPath()));
    String nestedJson = new String(encoded, "UTF-8");
    List<AzkabanJobExecRecord> result = ajc.parseJson(nestedJson, 11111);
    for (int i = 0; i < result.size(); i++) {
      AzkabanJobExecRecord aje = result.get(i);
      System.out.println(aje.getJobExecId());
      System.out.println(aje.getJobName());
      System.out.println(aje.getStartTime());
      System.out.println(aje.getEndTime());
      System.out.println(aje.getFlowPath());
      System.out.println();
      Assert.assertEquals((long) aje.getJobExecId(), 11111 * 1000 + i);
    }
  }



}
