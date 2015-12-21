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
package metadata.etl.dataset.hive;

import java.io.InputStream;
import java.util.Properties;
import metadata.etl.EtlJob;


/**
 * Created by zsun on 11/16/15.
 */
public class HiveMetadataEtl extends EtlJob {

  @Deprecated
  public HiveMetadataEtl(int dbId, long whExecId) {
    super(null, dbId, whExecId);
  }

  public HiveMetadataEtl(int dbId, long whExecId, Properties prop) {
    super(null, dbId, whExecId, prop);
  }


  @Override
  public void extract()
    throws Exception {
    logger.info("In Hive metadata ETL, launch extract jython scripts");
    InputStream inputStream = classLoader.getResourceAsStream("jython/HiveExtract.py");
    //logger.info("before call scripts " + interpreter.getSystemState().argv);
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void transform()
    throws Exception {
    logger.info("In Hive metadata ETL, launch transform jython scripts");
    InputStream inputStream = classLoader.getResourceAsStream("jython/HiveTransform.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void load()
    throws Exception {
      logger.info("In Hive metadata ETL, launch load jython scripts");
      InputStream inputStream = classLoader.getResourceAsStream("jython/HiveLoad.py");
      interpreter.execfile(inputStream);
      inputStream.close();
  }
}
