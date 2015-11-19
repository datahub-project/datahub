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
package wherehows.common.writers;

import wherehows.common.schemas.Record;

import java.io.*;


/**
 * Created by zsun on 8/20/15.
 */
public class FileWriter extends Writer {
  OutputStreamWriter streamWriter;

  public FileWriter(String fileName)
    throws FileNotFoundException {
    super();
    File f = new File(fileName);
    streamWriter = new OutputStreamWriter(new FileOutputStream(f));
  }

  @Override
  public boolean flush()
    throws IOException {
    for (Record r : this.records) {
      streamWriter.write(r.toCsvString() + "\n");
    }
    streamWriter.flush();
    this.records.clear();
    return false;
  }

  public void close()
    throws IOException {
    flush();
    this.streamWriter.close();
  }
}
