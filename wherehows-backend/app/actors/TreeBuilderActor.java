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
package actors;

import akka.actor.UntypedActor;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import metadata.etl.EtlJob;
import org.python.core.PyDictionary;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;
import play.Logger;
import play.Play;
import wherehows.common.Constant;
import wherehows.common.utils.JobsUtil;

import static wherehows.common.Constant.*;


/**
 * Created by zechen on 11/8/15.
 */
public class TreeBuilderActor extends UntypedActor {

  private static final String WH_DB_URL = Play.application().configuration().getString("db.wherehows.url");
  private static final String WH_DB_USERNAME = Play.application().configuration().getString("db.wherehows.username");
  private static final String WH_DB_PASSWORD = Play.application().configuration().getString("db.wherehows.password");
  private static final String WH_DB_DRIVER = Play.application().configuration().getString("db.wherehows.driver");

  public static final String ETL_JOBS_DIR = Play.application().configuration().getString(WH_ETL_JOBS_DIR);

  private static final String TREE_JOB_TYPE = "tree";

  private static Map<String, Properties> treeJobList = new HashMap<>();

  private PythonInterpreter interpreter;
  private PySystemState sys;

  @Override
  public void preStart() throws Exception {
    treeJobList = JobsUtil.getEnabledJobsByType(ETL_JOBS_DIR, TREE_JOB_TYPE);
  }

  @Override
  public void onReceive(Object o) throws Exception {
    if (!(o instanceof String)) {
      throw new Exception("message type is not supported!");
    }

    String jobName = (String) o;
    Logger.info("Start tree job {}", jobName);

    Properties props = treeJobList.get(jobName);
    if (props == null) {
      Logger.error("unknown job {}", jobName);
      return;
    }

    PyDictionary config = new PyDictionary();
    for (String key : props.stringPropertyNames()) {
      String value = props.getProperty(key);
      config.put(new PyString(key), new PyString(value));
    }
    config.put(new PyString(Constant.WH_DB_URL_KEY), new PyString(WH_DB_URL));
    config.put(new PyString(Constant.WH_DB_USERNAME_KEY), new PyString(WH_DB_USERNAME));
    config.put(new PyString(Constant.WH_DB_PASSWORD_KEY), new PyString(WH_DB_PASSWORD));
    config.put(new PyString(Constant.WH_DB_DRIVER_KEY), new PyString(WH_DB_DRIVER));

    sys = new PySystemState();
    sys.argv.append(config);
    interpreter = new PythonInterpreter(null, sys);

    String script = props.getProperty(Constant.JOB_SCRIPT_KEY);
    InputStream in = EtlJob.class.getClassLoader().getResourceAsStream(script);
    if (in != null) {
      interpreter.execfile(in);
      in.close();
      Logger.info("Finish tree job {}", jobName);
    } else {
      Logger.error("can not find jython script {}", script);
    }
  }
}
