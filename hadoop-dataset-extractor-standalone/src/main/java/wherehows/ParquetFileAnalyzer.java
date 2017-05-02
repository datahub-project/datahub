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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.SampleDataRecord;

public class ParquetFileAnalyzer extends FileAnalyzer {

  public ParquetFileAnalyzer(FileSystem fs) {
    super(fs);
    STORAGE_TYPE = "parquet";
  }

  @Override
  public DatasetJsonRecord getSchema(Path targetFilePath)
    throws IOException {
    System.out.println("parquet file path : " + targetFilePath.toUri().getPath());

    SeekableInput sin = new FsInput(targetFilePath, fs.getConf());
    ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(targetFilePath).build();

    String schemaString = reader.read().getSchema().toString();
    String storage = STORAGE_TYPE;
    String abstractPath = targetFilePath.toUri().getPath();

    FileStatus fstat = fs.getFileStatus(targetFilePath);
    // TODO set codec
    DatasetJsonRecord datasetJsonRecord =
      new DatasetJsonRecord(schemaString, abstractPath, fstat.getModificationTime(), fstat.getOwner(), fstat.getGroup(),
        fstat.getPermission().toString(), null, storage, "");
    reader.close();
    sin.close();
    return datasetJsonRecord;
  }

  @Override
  public SampleDataRecord getSampleData(Path targetFilePath)
    throws IOException {
    ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(targetFilePath).build();

    Iterator<GenericRecord> iter = Collections.singletonList(reader.read()).iterator();
    int count = 0;
    List<Object> list = new ArrayList<Object>();
    //JSONArray list = new JSONArray();
    while (iter.hasNext() && count < 10) {
      // TODO handle out of memory error
      list.add(iter.next().toString().replaceAll("[\\n\\r\\p{C}]", "").replaceAll("\"", "\\\""));
      count++;
    }
    SampleDataRecord sampleDataRecord = new SampleDataRecord(targetFilePath.toUri().getPath(), list);

    return sampleDataRecord;
  }
}
