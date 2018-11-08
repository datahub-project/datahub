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
import metadata.etl.LaunchJython;
import org.python.util.PythonInterpreter;
import org.testng.annotations.Test;


public class AvroColumnParserTest {

  @Test
  public void avroTest() {

    LaunchJython l = new LaunchJython();
    PythonInterpreter interpreter = l.setUp();

    ClassLoader classLoader = getClass().getClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream("jython-test/AvroColumnParserTest.py");
    interpreter.execfile(inputStream);

  }
}
