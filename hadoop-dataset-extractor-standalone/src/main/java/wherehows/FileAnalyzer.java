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

  /**
   * Decide the data source by check the full url of the dataset
   * @param fullPath
   * @return
   */
  protected static String checkDataSource(String fullPath) {
    String data_source = fullPath.matches("/(data|eidata)/tracking/.*") ? "Kafka"
      : fullPath.matches("/(data|eidata)/databases/.*") ? "Oracle"
        : fullPath.matches("/(data|eidata)/external/.*") ? "External"
          : fullPath.matches("/projects/dwh/dwh_.*|/jobs/data_svc/dwh_.*") ? "Teradata"
            : fullPath.matches("/projects/dwh/.*|/jobs/data_svc/.*") ? "Hdfs"
              : fullPath.matches("/.*/.*\\.db/") ? "Hive" : "Hdfs";
    return data_source;
  }

  public abstract DatasetJsonRecord getSchema(Path path)
    throws IOException;

  public abstract SampleDataRecord getSampleData(Path path)
    throws IOException;
}
