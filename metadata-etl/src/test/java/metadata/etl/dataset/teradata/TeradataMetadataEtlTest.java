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
package metadata.etl.dataset.teradata;

import org.testng.annotations.Test;


/**
 * Created by zechen on 9/10/15.
 */
public class TeradataMetadataEtlTest {

  @Test(groups = {"needConfig"})
  public void testRun()
    throws Exception {
    TeradataMetadataEtl t = new TeradataMetadataEtl(3, 0L);
    t.run();
  }

  @Test(groups = {"needConfig"})
  public void testExtract()
    throws Exception {
    TeradataMetadataEtl t = new TeradataMetadataEtl(3, 0L);
    t.extract();
  }
}
