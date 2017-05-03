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

import org.codehaus.jettison.json.JSONException;
import org.testng.Assert;
import org.testng.annotations.Test;
import wherehows.common.schemas.AzkabanJobExecRecord;
import wherehows.common.schemas.LineageRecord;

import java.util.List;


/**
 * Created by zsun on 9/9/15.
 */
public class AzJsonAnalyzerTest {

  @Test
  public void extractFromJsonTest()
    throws JSONException {
    String jsonString =
      "{\"conf\":{\"path\":\"hdfs://eat1-nertznn01.grid.linkedin.com:9000/system/mr-history/finished/2015/09/07/000140/job_1441257279406_140910_conf.xml\","
        +
        "\"property\":[{\"name\":\"mapreduce.jobtracker.address\"," +
        "\"value\":\"local\"," +
        "\"source\":[\"mapred-default.xml\",\"file:/grid/c/tmp/yarn/usercache/dev_svc/appcache/application_1441257279406_140910/filecache/13/job.xml\",\"job.xml\",\"hdfs://eat1-nertznn01.grid.linkedin.com:9000/system/mr-history/finished/2015/09/07/000140/job_1441257279406_140910_conf.xml\"]},"
        +
        "{\"name\":\"mapreduce.input.fileinputformat.inputdir\",\"value\":\"hdfs://eat1-nertznn01.grid.linkedin.com:9000/data/hadoop/lsr/lva1-war/2015/09/lsrout.2015-09-07\","
        +
        "\"source\":[\"something\"]}" +
        " ]" +
        " }}";

    AzkabanJobExecRecord azkabanJobExecRecord =
      new AzkabanJobExecRecord(-1, "someJobName", new Long(0), 0, 0, "S", "path");
    azkabanJobExecRecord.setJobExecId((long) 111);
    AzJsonAnalyzer efc = new AzJsonAnalyzer(jsonString, azkabanJobExecRecord, -1);
    List<LineageRecord> results = efc.extractFromJson();
    Assert.assertEquals(results.get(0).getFullObjectName(),
      "hdfs://eat1-nertznn01.grid.linkedin.com:9000/data/hadoop/lsr/lva1-war/2015/09/lsrout.2015-09-07");
    //Assert.assertEquals(results.get(0).toDatabaseValue(),
    //  "'-1','0','111',NULL,'someJobName','0','0','-1',NULL,'hdfs://eat1-nertznn01.grid.linkedin.com:9000/data/hadoop/lsr/lva1-war/2015/09/lsrout.2015-09-07',NULL,NULL,NULL,NULL,'HDFS','source',NULL,NULL,'read',NULL,NULL,NULL,NULL,'path',NULL,NULL");
  }
}
