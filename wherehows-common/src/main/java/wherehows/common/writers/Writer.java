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

import java.sql.SQLException;
import wherehows.common.schemas.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by zsun on 8/20/15.
 */
public abstract class Writer {
  int MAX_LENGTH = 1000; // TODO need to be the real size, instead of the record number
  boolean AUTO_WRITE = true;
  List<Record> records;

  public Writer() {
    records = new ArrayList<Record>();
  }

  /**
   * Write a record to the writer
   * @param record
   */
  public synchronized void append(Record record)
    throws IOException, SQLException {
    records.add(record);
    // check if the size meet the threhold, flush it
    if (AUTO_WRITE && records.size() > MAX_LENGTH) {
      flush();
    }
  }

  public abstract boolean flush()
    throws IOException, SQLException;

  public abstract void close()
    throws IOException, SQLException;
}
