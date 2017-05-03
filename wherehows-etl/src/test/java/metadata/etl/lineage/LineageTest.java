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

import org.testng.annotations.Test;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.LoggerFactory;


/**
 * Created by zsun on 8/28/15.
 */
public class LineageTest {
  public Properties properties;
  AzLineageMetadataEtl lm;

  public LineageTest() {
    lm = new AzLineageMetadataEtl(31);
    properties = lm.prop;
  }

  @Test(groups = {"needConfig"})
  public void testLineage()
    throws Exception {
    lm.run();
  }

  @Test(groups = {"needConfig"})
  public void logTest() {
    Logger logger = LoggerFactory.getLogger(LineageTest.class);
    logger.debug("asfsd");
  }
}
