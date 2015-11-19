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
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;
import wherehows.common.DatasetPath;


/**
 * Created by zsun on 11/4/15.
 */
public class DatasetPathTest {

  @Test
  public void separatedDataset() {
    String sample = "/jobs/search/search-metrics/core-metrics/sessions/daily/{2015/10/20,2017}/desktop, /data/something/{1232,422}, /qfewfs/afasd/zxc";
    List<String> result = DatasetPath.separatedDataset(sample);
    String[] expecteddResult = {"/jobs/search/search-metrics/core-metrics/sessions/daily/{2015/10/20,2017}/desktop",
    "/data/something/{1232,422}", "/qfewfs/afasd/zxc"};
    for (int i = 0; i < result.size(); i++) {
      Assert.assertEquals(result.get(i), expecteddResult[i]);
    }

  }

  @Test
  public void seperatedDatasetTest2() {
    String sample2 = "/.{/jobs/snusm/online/modeling/train-data/slot-1,/jobs/snusm/online/modeling/test-data/slot-1}";
    List<String> result2 = DatasetPath.separatedDataset(sample2);
    Assert.assertEquals(result2.get(0), sample2);
  }
}
