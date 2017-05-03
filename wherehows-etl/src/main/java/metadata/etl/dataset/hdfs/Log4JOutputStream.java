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
package metadata.etl.dataset.hdfs;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Redirection of output stream to log4j
 * @author kdelfour
 *
 */
public class Log4JOutputStream extends OutputStream {

  // The default logger
  private static final Logger Logger =  LoggerFactory.getLogger("Redirect schemaFetch output : ");

  private final StringBuilder stringBuilder = new StringBuilder();

  /*
   * (non-Javadoc)
   *
   * @see java.io.OutputStream#write(int)
   */
  @Override
  public void write(int b)
      throws IOException {
    char current = (char) b;
    if (current == '\n') {
      Logger.info(stringBuilder.toString());
      // Reset it
      stringBuilder.setLength(0);
    } else {
      stringBuilder.append(current);
    }
  }
}