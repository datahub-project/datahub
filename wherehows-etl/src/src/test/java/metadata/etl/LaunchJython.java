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
import java.net.URL;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;


/**
 * Created by zsun on 3/9/16.
 */
public class LaunchJython {

  public PythonInterpreter setUp() {
    PySystemState sys = new PySystemState();
    addJythonToPath(sys);
    return new PythonInterpreter(null, sys);
  }

  /**
   * Need to add the jython scripts in main/resource/jython into the PySystemState
   * Note the jython resource folder name in test must be different from the one in main,
   * otherwise it will overwrite the path.
   * @param pySystemState
   */
  private void addJythonToPath(PySystemState pySystemState) {
    URL url = getClass().getClassLoader().getResource("jython");
    if (url != null) {
      File file = new File(url.getFile());
      String path = file.getPath();
      System.out.println(path);
      if (path.startsWith("file:")) {
        path = path.substring(5);
      }
      pySystemState.path.append(new PyString(path.replace("!", "")));
    }
  }
}
