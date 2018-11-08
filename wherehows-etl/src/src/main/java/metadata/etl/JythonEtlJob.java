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
package metadata.etl;

import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import wherehows.common.Constant;

@Slf4j
public class JythonEtlJob extends EtlJob {

  public JythonEtlJob(long whExecId, Properties prop) {
    super(whExecId, prop);
  }

  @Override
  public void extract() throws Exception {
    String script = prop.getProperty(Constant.JOB_JYTHON_EXTRACT_KEY);
    if (script == null) {
      log.info("Skipped extract as no script is defined in {}", Constant.JOB_JYTHON_EXTRACT_KEY);
      return;
    }

    runScript(script);
  }

  @Override
  public void transform() throws Exception {
    String script = prop.getProperty(Constant.JOB_JYTHON_TRANSFORM_KEY);
    if (script == null) {
      log.info("Skipped transform as no script is defined in {}", Constant.JOB_JYTHON_TRANSFORM_KEY);
      return;
    }

    runScript(script);
  }

  @Override
  public void load() throws Exception {
    String script = prop.getProperty(Constant.JOB_JYTHON_LOAD_KEY);
    if (script == null) {
      log.info("Skipped load as no script is defined in {}", Constant.JOB_JYTHON_LOAD_KEY);
      return;
    }

    runScript(script);
  }

  private void runScript(String scriptFile) throws Exception {
    log.info("Launching jython script {}", scriptFile);

    try (InputStream inputStream = classLoader.getResourceAsStream(scriptFile)) {
      interpreter.execfile(inputStream);
      inputStream.close();
    }
  }
}
