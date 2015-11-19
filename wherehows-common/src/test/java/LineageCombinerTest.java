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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import metadata.etl.lineage.AzLineageMetadataEtl;
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
public class LineageCombinerTest {
  LineageCombiner lineageCombiner;
  @BeforeTest
  public void setUp()
    throws SQLException {
    AzLineageMetadataEtl azLineageMetadataEtl = new AzLineageMetadataEtl(0); // to get the config
    String wherehowsHost = azLineageMetadataEtl.prop.getProperty("wherehows.db.host");
    String wherehowsUserName = azLineageMetadataEtl.prop.getProperty("wherehows.db.username");
    String wherehowsPassWord = azLineageMetadataEtl.prop.getProperty("wherehows.db.password");
    Connection conn =
      DriverManager.getConnection(wherehowsHost + "?" + "user=" + wherehowsUserName + "&password=" + wherehowsPassWord);

    lineageCombiner = new LineageCombiner(conn);
    PathAnalyzer.initialize(conn);
  }

  @Test
  public void lineageCombinerTest() {
    LineageRecord lineageRecord = new LineageRecord(0, 111L, "some_job_name", 111001L);
    lineageRecord.setDatasetInfo(0,
      "hdfs://eat1-nertznn01.grid.linkedin.com:9000/data/hadoop/someDataSet/daily/2015/09/01/part1", "");
    lineageRecord.setOperationInfo("source", "read",null, null, null, null, 0, 0, "someFlowPath");

    LineageRecord lineageRecord2 = new LineageRecord(0, 111L, "some_job_name", 111001L);
    lineageRecord2.setDatasetInfo(0,
      "hdfs://eat1-nertznn01.grid.linkedin.com:9000/data/hadoop/someDataSet/daily/2015/09/02/part1", "");
    lineageRecord2.setOperationInfo("source", "read",null, null, null, null, 0, 0, "someFlowPath");

    LineageRecord lineageRecord3 = new LineageRecord(0, 111L, "some_job_name", 111001L);
    lineageRecord3.setDatasetInfo(0,
      "hdfs://eat1-nertznn01.grid.linkedin.com:9000/data/hadoop/someDataSet/daily/2015/09/02/part3", "");
    lineageRecord3.setOperationInfo("source", "read",null, null, null, null, 0, 0, "someFlowPath");

    List<LineageRecord> allLineage = new ArrayList<>();
    allLineage.add(lineageRecord);
    allLineage.add(lineageRecord2);
    allLineage.add(lineageRecord3);

    lineageCombiner.addAll(allLineage);

    System.out.println(lineageCombiner.getCombinedLineage().get(0).toDatabaseValue());

    Assert.assertEquals(lineageCombiner.getCombinedLineage().get(0).toDatabaseValue(),
      "'some_app_name','0','111','0','111001','some_job_name','0','0','some_database_name','0','/data/hadoop/someDataSet','hdfs://eat1-nertznn01.grid.linkedin.com:9000/data/hadoop/someDataSet/daily/2015/09/01/part1','2015/09/01','2015/09/02','daily','1','','source','0','0','read','0','0','0','0',NULL,NULL");
  }

  @Test
  public void analyzeTest() {
    String fullPath = "hdfs://lva1-warnn01.grid.linkedin.com:9000/jobs/siteflow/scoring/score/abook_decision/heathrowInactive/part-m-00004";
    DatasetPath datasetPath = PathAnalyzer.analyze(fullPath);
    Assert.assertEquals(datasetPath.abstractPath, "/jobs/siteflow/scoring/score/abook_decision/heathrowInactive");
  }
}
