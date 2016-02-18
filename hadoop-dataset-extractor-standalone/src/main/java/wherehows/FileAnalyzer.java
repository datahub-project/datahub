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
package wherehows;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.DatasetSchemaRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.IOException;


/**
 * Created by zsun on 8/18/15.
 */
public abstract class FileAnalyzer {
  String STORAGE_TYPE;
  FileSystem fs;

  public FileAnalyzer(FileSystem fs) {
    this.fs = fs;
  }

  public abstract DatasetJsonRecord getSchema(Path path)
    throws IOException;

  public abstract SampleDataRecord getSampleData(Path path)
    throws IOException;
}
