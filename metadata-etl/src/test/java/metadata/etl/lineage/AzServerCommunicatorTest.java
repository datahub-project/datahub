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

import java.util.Properties;
import java.util.Set;
import org.junit.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;
import wherehows.common.Constant;


/**
 * Created by zsun on 8/29/15.
 */
public class AzServerCommunicatorTest {
  AzServiceCommunicator asc;
  Properties prop;

  @BeforeTest
  public void setUp()
    throws Exception {
    this.prop = new LineageTest().properties;
    asc = new AzServiceCommunicator(prop);
  }

  @Test(groups = {"needConfig"})
  public void testGetExecLog()
    throws Exception {

    int execId = 758434;
    String jobId = "azkaban-log_load-azkaban-log";
    String offset = "0";
    String length = "10";

    String log = asc.getExecLog(execId, jobId, offset, length);
    Assert.assertEquals("27-08-2015", log);
  }

  @Test(groups = {"needConfig"})
  public void testGetSessionId()
    throws Exception {

    String azkabanUserName = prop.getProperty(Constant.AZ_SERVICE_USERNAME_KEY);
    String azkabanPassword = prop.getProperty(Constant.AZ_SERVICE_PASSWORD_KEY);
    String response = asc.getAzkabanSessionId(azkabanUserName, azkabanPassword);
    System.out.println(response);
    Assert.assertTrue(!response.equals(""));
  }

  @Test(groups = {"needConfig"})
  public void getHadoopID()
    throws Exception {
    int execId = 843164;
    String jobId = "creativesCompaniesPreJoinCreatives";
    String offset = "0";
    String length = "100000";

    String log = asc.getExecLog(execId, jobId, offset, length);
    AzLogParser alp = new AzLogParser();
    Set<String> hadoopJobId = alp.getHadoopJobIdFromLog(log);

    System.out.println(hadoopJobId.iterator().next());
    Assert.assertEquals(hadoopJobId.iterator().next(), "job_1441257279406_483695");
  }
}
