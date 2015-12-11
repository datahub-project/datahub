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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Created by zechen on 12/8/15.
 */
public class GitMetadataEtlTest {
  GitMetadataEtl git;

  @BeforeMethod
  public void setUp()
      throws Exception {
    this.git = new GitMetadataEtl(500, 0L);
  }

  @Test
  public void testExtract()
      throws Exception {
    git.extract();
  }

  @Test
  public void testTransform()
      throws Exception {
    git.transform();
  }

  @Test
  public void testLoad()
      throws Exception {
    git.load();
  }

  @Test
  public void testRun()
      throws Exception {
    git.run();
  }
}