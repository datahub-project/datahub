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
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import wherehows.common.Constant;
import wherehows.common.schemas.AzkabanJobExecRecord;
import wherehows.common.schemas.LineageRecord;


/**
 * Created by zsun on 9/24/15.
 */
public class AzLogParserTest {

  private final int TEST_APP_ID = -1;
  private final int TEST_DATABASE_ID = -1;
  @BeforeTest
  public void setUp()
    throws SQLException {
    Properties prop = new LineageTest().properties;

    String wherehowsHost = prop.getProperty(Constant.WH_DB_URL_KEY);
    String wherehowsUserName = prop.getProperty(Constant.WH_DB_USERNAME_KEY);
    String wherehowsPassWord = prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
    Connection conn =
      DriverManager.getConnection(wherehowsHost + "?" + "user=" + wherehowsUserName + "&password=" + wherehowsPassWord);
    AzLogParser.initialize(conn);
  }

  @Test(groups = {"needConfig"})
  public void getHadoopJobIdFromLogTest() {
    String logSample = "9-08-2015 03:02:16 PDT hadoop-datasets-stats_sizeAggr INFO - INFO  map 26% reduce 0%\n" +
      "29-08-2015 03:02:16 PDT hadoop-datasets-stats_sizeAggr INFO - INFO  map 30% reduce 0%\n" +
      "29-08-2015 03:02:17 PDT hadoop-datasets-stats_sizeAggr INFO - INFO Job job_1440264275625_235896 completed successfully\n"
      +
      "29-08-2015 03:02:17 PDT hadoop-datasets-stats_sizeAggr INFO - INFO  map 29% reduce 0%\n" +
      "29-08-2015 03:02:17 PDT hadoop-datasets-stats_sizeAggr INFO - INFO Job job_1440264275625_235886 completed successfully\n"
      +
      "29-08-2015 03:02:17 PDT hadoop-datasets-stats_sizeAggr INFO - INFO  map 19% reduce 0%";
    Set<String> hadoopJobId = AzLogParser.getHadoopJobIdFromLog(logSample);
    String[] expectedJobId = new String[]{"job_1440264275625_235886", "job_1440264275625_235896"};
    Assert.assertEquals(hadoopJobId.toArray(), expectedJobId);

    String hiveLog = "07-10-2015 14:38:47 PDT hive-with-hiveconf INFO - 15/10/07 21:38:47 INFO impl.YarnClientImpl: Submitted application application_1443068642861_495047\n"
      + "07-10-2015 14:38:47 PDT hive-with-hiveconf INFO - 15/10/07 21:38:47 INFO mapreduce.Job: The url to track the job: http://eat1-nertzwp01.grid.linkedin.com:8080/proxy/application_1443068642861_495047/\n"
      + "07-10-2015 14:38:47 PDT hive-with-hiveconf INFO - Starting Job = job_1443068642861_495047, Tracking URL = http://eat1-nertzwp01.grid.linkedin.com:8080/proxy/application_1443068642861_495047/\n"
      + "07-10-2015 14:38:47 PDT hive-with-hiveconf INFO - 15/10/07 21:38:47 INFO exec.Task: Starting Job = job_1443068642861_495047, Tracking URL = http://eat1-nertzwp01.grid.linkedin.com:8080/proxy/application_1443068642861_495047/\n"
      + "07-10-2015 14:38:47 PDT hive-with-hiveconf INFO - Kill Command = /export/apps/hadoop/latest/bin/hadoop job  -kill job_1443068642861_495047\n"
      + "07-10-2015 14:38:47 PDT hive-with-hiveconf INFO - 15/10/07 21:38:47 INFO exec.Task: Kill Command = /export/apps/hadoop/latest/bin/hadoop job  -kill job_1443068642861_495047\n"
      + "07-10-2015 14:38:55 PDT hive-with-hiveconf INFO - Hadoo";
    hadoopJobId = AzLogParser.getHadoopJobIdFromLog(hiveLog);

    expectedJobId = new String[]{"job_1443068642861_495047"};
    Assert.assertEquals(hadoopJobId.toArray(), expectedJobId);

  }

  @Test(groups = {"needConfig"})
  public void getLineageFromLogTest() {
    String logSample = "asfdasdfsadf Moving from staged path[asdf] to final resting place[/tm/b/c] sdaf dsfasdfasdf";
    AzkabanJobExecRecord sampleExecution = new AzkabanJobExecRecord(TEST_APP_ID, "someJobName", (long) 0, 0, 0, "S", "path");
    sampleExecution.setJobExecId((long) 11111);
    List<LineageRecord> result = AzLogParser.getLineageFromLog(logSample, sampleExecution, -1);
    System.out.println(result.get(0).toDatabaseValue());

    Assert.assertEquals(result.get(0).toDatabaseValue(),
      "'-1','0','11111',NULL,'someJobName','0','0','-1',NULL,'/tm/b/c',NULL,NULL,NULL,NULL,'HDFS','target',NULL,NULL,'write','0','0','0','0','path',NULL,NULL");
  }

  @Test(groups = {"needConfig"})
  public void getLineageFromLogTest2() {
    String logSample = "Estimated disk size for store endorsements-member-restrictions in node Node lva1-app1508.prod.linkedin.com [id 39] in KB: 90916\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO Checksum for node 39 - f17b2f57adb0595e80ea86c0dd997fc0\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO Setting permission to 755 for /jobs/endorse/endorsements/master/tmp/endorsements-member-restrictions.store/lva1-voldemort-read-only-2-vip.prod.linkedin.com/node-39/.metadata\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO Pushing to cluster URL: tcp://lva1-voldemort-read-only-2-vip.prod.linkedin.com:10103\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO StorePushTask.call() invoked for cluster URL: tcp://lva1-voldemort-read-only-2-vip.prod.linkedin.com:10103\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - WARN The server requested pushHighAvailability to be DISABLED on cluster: tcp://lva1-voldemort-read-only-2-vip.prod.linkedin.com:10103\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO Push starting for cluster: tcp://lva1-voldemort-read-only-2-vip.prod.linkedin.com:10103\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO tcp://lva1-voldemort-read-only-2-vip.prod.linkedin.com:10103 : Existing protocol = hdfs and port = 9000\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO tcp://lva1-voldemort-read-only-2-vip.prod.linkedin.com:10103 : New protocol = webhdfs and port = 50070\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO Client zone-id [-1] Attempting to get raw store [voldsys$_metadata_version_persistence] \n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO Client zone-id [-1] Attempting to get raw store [voldsys$_store_quotas] \n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO tcp://lva1-voldemort-read-only-2-vip.prod.linkedin.com:10103 : Initiating swap of endorsements-member-restrictions with dataDir: /jobs/endorse/endorsements/master/tmp/endorsements-member-restrictions.store/lva1-voldemort-read-only-2-vip.prod.linkedin.com\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO tcp://lva1-voldemort-read-only-2-vip.prod.linkedin.com:10103 : Invoking fetch for Node lva1-app0610.prod.linkedin.com [id 0] for webhdfs://lva1-warnn01.grid.linkedin.com:50070/jobs/endorse/endorsements/master/tmp/endorsements-member-restrictions.store/lva1-voldemort-read-only-2-vip.prod.linkedin.com/node-0\n"
      + "17-11-2015 01:32:27 PST endorsements_push-lva-endorsements-member-restrictions INFO - INFO tcp://lva1-voldemort-rea";
    AzkabanJobExecRecord sampleExecution = new AzkabanJobExecRecord(TEST_APP_ID, "someJobName", (long) 0, 0, 0, "S", "path");
    List<LineageRecord> result = AzLogParser.getLineageFromLog(logSample, sampleExecution, TEST_DATABASE_ID);
    System.out.println(result.get(0).toDatabaseValue());
    Assert.assertEquals(result.get(0).getFullObjectName(),
      "tcp://lva1-voldemort-read-only-2-vip.prod.linkedin.com:10103/endorsements-member-restrictions");
  }

}
