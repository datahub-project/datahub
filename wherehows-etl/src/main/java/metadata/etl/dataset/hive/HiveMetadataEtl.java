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
import lombok.extern.slf4j.Slf4j;
import metadata.etl.EtlJob;
import wherehows.common.Constant;


/**
 * Created by zsun on 11/16/15.
 */
@Slf4j
public class HiveMetadataEtl extends EtlJob {

  public HiveMetadataEtl(long whExecId, Properties prop) {
    super(whExecId, prop);
  }

  @Override
  public void extract()
    throws Exception {
    log.info("In Hive metadata ETL, launch extract jython scripts");

    System.setProperty("java.security.krb5.realm", prop.getProperty(Constant.KRB5_REALM));
    System.setProperty("java.security.krb5.kdc", prop.getProperty(Constant.KRB5_KDC));

    InputStream inputStream = classLoader.getResourceAsStream("jython/HiveExtract.py");
    //logger.info("before call scripts " + interpreter.getSystemState().argv);
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void transform()
    throws Exception {
    log.info("In Hive metadata ETL, launch transform jython scripts");
    InputStream inputStream = classLoader.getResourceAsStream("jython/HiveTransform.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void load()
    throws Exception {
      log.info("In Hive metadata ETL, launch load jython scripts");
      InputStream inputStream = classLoader.getResourceAsStream("jython/HiveLoad.py");
      interpreter.execfile(inputStream);
      inputStream.close();
  }
}
