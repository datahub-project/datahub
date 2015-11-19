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
import java.util.ArrayList;
import java.util.List;


/**
 * Created by zsun on 8/18/15.
 */
public class FileAnalyzerFactory {
  List<FileAnalyzer> allFileAnalyzer;

  public FileAnalyzerFactory(FileSystem fs) {
    allFileAnalyzer = new ArrayList<FileAnalyzer>();
    allFileAnalyzer.add(new AvroFileAnalyzer(fs));
    // allFileAnalyzer.add(new OrcFileAnalyzer(fs));
    // linkedin specific
    // allFileAnalyzer.add(new BinaryJsonFileAnalyzer(fs));
  }

  // iterate through all possibilities
  public SampleDataRecord getSampleData(Path path, String abstractPath) {
    SampleDataRecord sampleData = null;
    for (FileAnalyzer fileAnalyzer : allFileAnalyzer) {
      try {
        sampleData = fileAnalyzer.getSampleData(path);
        sampleData.setAbstractPath(abstractPath);
      } catch (Exception ignored) {
      }
      if (sampleData != null) {
        break;
      }
    }
    return sampleData;
  }

  public DatasetJsonRecord getSchema(Path path, String abstractPath)
    throws IOException {
    DatasetJsonRecord schema = null;
    for (FileAnalyzer fileAnalyzer : allFileAnalyzer) {
      System.out.println("try file analyzer: " + fileAnalyzer.STORAGE_TYPE);
      try {
        schema = fileAnalyzer.getSchema(path);
        schema.setAbstractPath(abstractPath);
      } catch (Exception ignored) {
        System.out.println("Debug: " + ignored);
      }
      if (schema != null) {
        break;
      }
    }
    return schema;
  }
}
