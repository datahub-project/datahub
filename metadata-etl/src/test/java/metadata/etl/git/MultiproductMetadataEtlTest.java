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
package metadata.etl.git;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class MultiproductMetadataEtlTest {
  MultiproductMetadataEtl _etl;

  @BeforeTest
  public void setUp()
      throws Exception {
    _etl = new MultiproductMetadataEtl(500, 0L);
  }

  @Test
  public void extractTest()
      throws Exception {
    _etl.extract();
    // check the csv file
  }

  @Test
  public void loadTest()
      throws Exception {
    _etl.load();
    // check in database
  }

  @Test
  public void runTest()
      throws Exception {
    extractTest();
    //transformTest();
    loadTest();
  }

}
