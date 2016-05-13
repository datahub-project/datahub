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

import junit.framework.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


/**
 * Created by zsun on 9/3/15.
 */
public class HadoopJobHistoryNodeExtractorTest {
  HadoopJobHistoryNodeExtractor he;

  @BeforeTest
  public void setUp() {
    try {
      he = new HadoopJobHistoryNodeExtractor(new LineageTest().properties);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test(groups = {"needConfig"})
  public void testGetConf()
    throws Exception {
    String result = he.getConfFromHadoop("job_1437229398924_817615");
    // job_1437229398924_817551
    System.err.println(result); // always will not found for old job
    Assert.assertNotNull(result);
  }
}
