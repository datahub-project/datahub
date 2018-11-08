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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.python.core.PyDictionary;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;
import wherehows.common.Constant;
import wherehows.common.jobs.BaseJob;


/**
 * {@code EtlJob} is the interface of all ETL job.
 * It handle the Jython classpath and all configuration process.
 * Each ETL process that implement this interface will have their own extract, transform, load function.
 * Created by zsun on 7/29/15.
 */
@Slf4j
public abstract class EtlJob extends BaseJob {

  public final ClassLoader classLoader = getClass().getClassLoader();

  public PythonInterpreter interpreter;

  /**
   * Used by backend service
   * @param whExecId
   * @param properties
   */
  public EtlJob(Long whExecId, Properties properties) {
    super(whExecId, properties);

    PySystemState sys = configFromProperties();
    addJythonToPath(sys);
    interpreter = new PythonInterpreter(null, sys);
  }

  private void addJythonToPath(PySystemState pySystemState) {
    Enumeration<URL> urls;
    try {
      urls = classLoader.getResources("jython/");
    } catch (IOException e) {
      log.info("Failed to get resource: {}", e.getMessage());
      return;
    }

    while (urls.hasMoreElements()) {
      URL url = urls.nextElement();
      log.debug("jython url: {}", url.getPath());
      if (url != null) {
        File file = new File(url.getFile());
        String path = file.getPath();
        if (path.startsWith("file:")) {
          path = path.substring(5);
        }
        pySystemState.path.append(new PyString(path.replace("!", "")));
      }
    }
  }

  /**
   * Copy all properties into jython environment
   * @return PySystemState A PySystemState that contain all the arguments.
   */
  private PySystemState configFromProperties() {
    final PyDictionary config = new PyDictionary();
    for (String key : prop.stringPropertyNames()) {
      config.put(new PyString(key), new PyString(prop.getProperty(key)));
    }
    PySystemState sys = new PySystemState();
    sys.argv.append(config);
    return sys;
  }

  public abstract void extract() throws Exception;

  public abstract void transform() throws Exception;

  public abstract void load() throws Exception;

  public void setup() throws Exception {
    // redirect error to out
    System.setErr(System.out);
  }

  public void close() throws Exception {
    interpreter.cleanup();
    interpreter.close();
  }

  public void run() throws Exception {
    setup();
    log.info("PySystem path: " + interpreter.getSystemState().path.toString());
    extract();
    transform();
    load();
    close();
  }
}
