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
  public void parseNestedJsonTest()
      throws IOException, URISyntaxException {

    URL url = Thread.currentThread().getContextClassLoader().getResource("nestedFlowContent.json");
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
