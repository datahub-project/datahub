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
package metadata.etl.models;

import java.util.Properties;
import metadata.etl.EtlJob;


public class DummyEtlJob extends EtlJob {

  public final long whExecId;
  public final Properties properties;

  public DummyEtlJob(long whExecId, Properties prop) {
    super(whExecId, prop);
    this.whExecId = whExecId;
    this.properties = prop;
  }

  @Override
  public void extract() throws Exception {

  }

  @Override
  public void transform() throws Exception {

  }

  @Override
  public void load() throws Exception {

  }
}
