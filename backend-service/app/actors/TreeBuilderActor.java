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
import metadata.etl.EtlJob;
import models.daos.EtlJobPropertyDao;
import org.python.core.PyDictionary;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;
import play.Logger;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zechen on 11/8/15.
 */
public class TreeBuilderActor extends UntypedActor {

  public PythonInterpreter interpreter;
  public PySystemState sys;

  @Override
  public void onReceive(Object o) throws Exception {
    if (o instanceof String) {
      Properties whProps = EtlJobPropertyDao.getWherehowsProperties();
      PyDictionary config = new PyDictionary();
      for (String key : whProps.stringPropertyNames()) {
        String value = whProps.getProperty(key);
        config.put(new PyString(key), new PyString(value));
      }
      sys = new PySystemState();
      sys.argv.append(config);
      interpreter = new PythonInterpreter(null, sys);
      String msg = (String) o;
      Logger.info("Start build {} tree", msg);
      InputStream in = null;
      switch (msg) {
        case "dataset":
          in = EtlJob.class.getClassLoader().getResourceAsStream("jython/DatasetTreeBuilder.py");
          break;
        case "flow":
          //in = EtlJob.class.getClassLoader().getResourceAsStream("jython/FlowTreeBuilder.py");
          break;
        default:
          Logger.error("unknown message : {}", msg);
      }
      if (in != null) {
        interpreter.execfile(in);
        in.close();
        Logger.info("Finish build {} tree", msg);
      } else {
        Logger.error("can not find jython script");
      }
    } else {
      throw new Exception("message type is not supported!");
    }
  }
}
