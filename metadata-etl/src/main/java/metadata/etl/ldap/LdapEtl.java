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
package metadata.etl.ldap;

import java.io.InputStream;
import java.util.Properties;
import metadata.etl.EtlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by zechen on 11/23/15.
 */
public class LdapEtl extends EtlJob {
  public ClassLoader classLoader = getClass().getClassLoader();
  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public LdapEtl(int appId, long whExecId) {
    super(appId, null, whExecId);
  }

  public LdapEtl(int appId, long whExecId, Properties prop) {
    super(appId, null, whExecId, prop);
  }

  public void extract() throws Exception {
    logger.info("ldap db extract");
    // call a python script to do the extraction
    InputStream inputStream = classLoader.getResourceAsStream("jython/LdapExtract.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void transform()
      throws Exception {
    logger.info("ldap db transform");
    // call a python script to do the transformation
    InputStream inputStream = classLoader.getResourceAsStream("jython/LdapTransform.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }

  @Override
  public void load()
      throws Exception {
    logger.info("ldap db load");
    // call a python script to do the loading
    InputStream inputStream = classLoader.getResourceAsStream("jython/LdapLoad.py");
    interpreter.execfile(inputStream);
    inputStream.close();
  }
}
