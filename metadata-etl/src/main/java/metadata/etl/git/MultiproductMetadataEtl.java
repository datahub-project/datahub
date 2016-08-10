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

import java.io.InputStream;
import java.util.Properties;
import metadata.etl.EtlJob;


public class MultiproductMetadataEtl extends EtlJob {

  @Deprecated
  public MultiproductMetadataEtl(int appId, long whExecId) {
    super(appId, null, whExecId);
  }

  public MultiproductMetadataEtl(int appId, long whExecId, Properties prop) {
    super(appId, null, whExecId, prop);
  }


  @Override
  public void extract()
      throws Exception {
    logger.info("In Multiproduct metadata ETL, launch extract jython scripts");
    InputStream inputStream = classLoader.getResourceAsStream("jython/MultiproductExtract.py");
    // logger.info("call scripts with args: " + interpreter.getSystemState().argv);
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void transform()
      throws Exception {
  }

  @Override
  public void load()
      throws Exception {
    logger.info("In Multiproduct metadata ETL, launch load jython scripts");
    InputStream inputStream = classLoader.getResourceAsStream("jython/MultiproductLoad.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }
}
