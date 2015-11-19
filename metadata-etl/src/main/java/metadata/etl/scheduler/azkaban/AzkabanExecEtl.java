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
package metadata.etl.scheduler.azkaban;

import java.io.InputStream;
import java.util.Properties;
import metadata.etl.EtlJob;


/**
 * Created by zechen on 8/20/15.
 */
public class AzkabanExecEtl extends EtlJob {

  public AzkabanExecEtl(int appId, long whExecId) {
    super(appId, null, whExecId);
  }

  public AzkabanExecEtl(int appId, long whExecId, Properties properties) {
    super(appId, null, whExecId, properties);
  }

  @Override
  public void extract()
    throws Exception {
    logger.info("In AzkabanExecEtl java launch extract jython scripts");
    InputStream inputStream = classLoader.getResourceAsStream("jython/AzkabanExtract.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void transform()
    throws Exception {
    logger.info("In AzkabanExecEtl java launch transform jython scripts");
    InputStream inputStream = classLoader.getResourceAsStream("jython/AzkabanTransform.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void load()
    throws Exception {
    logger.info("In AzkabanExecEtl java launch load jython scripts");
    InputStream inputStream = classLoader.getResourceAsStream("jython/AzkabanLoad.py");
    interpreter.execfile(inputStream);
    inputStream.close();
    logger.info("In AzkabanExecEtl java load jython scripts finished");
  }
}
