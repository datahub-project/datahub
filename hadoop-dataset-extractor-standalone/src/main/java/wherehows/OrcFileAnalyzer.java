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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by zsun on 8/18/15.
 */
public class OrcFileAnalyzer extends FileAnalyzer {

  public OrcFileAnalyzer(FileSystem fs) {
    super(fs);
    STORAGE_TYPE = "orc";
  }

  @Override
  public DatasetJsonRecord getSchema(Path targetFilePath)
    throws IOException {
    Reader orcReader = OrcFile.createReader(fs, targetFilePath);
    String codec = String.valueOf(orcReader.getCompression());
    String schemaString = orcReader.getObjectInspector().getTypeName();
    String storage = STORAGE_TYPE;
    String abstractPath = targetFilePath.toUri().getPath();

    FileStatus fstat = fs.getFileStatus(targetFilePath);
    DatasetJsonRecord datasetJsonRecord =
      new DatasetJsonRecord(schemaString, abstractPath, fstat.getModificationTime(), fstat.getOwner(), fstat.getGroup(),
        fstat.getPermission().toString(), codec, storage, "");

    return datasetJsonRecord;
  }

  @Override
  public SampleDataRecord getSampleData(Path targetFilePath)
    throws IOException {
    Reader orcReader = OrcFile.createReader(fs, targetFilePath);
    RecordReader recordReader = orcReader.rows();
    int count = 0;
    List<Object> list = new ArrayList<Object>();
    Object row = null;
    while (recordReader.hasNext() && count < 10) {
      count++;
      row = recordReader.next(row);
      list.add(row.toString().replaceAll("[\\n\\r\\p{C}]", ""));
    }
    SampleDataRecord sampleDataRecord = new SampleDataRecord(targetFilePath.toUri().getPath(), list);
    return sampleDataRecord;
  }
}
