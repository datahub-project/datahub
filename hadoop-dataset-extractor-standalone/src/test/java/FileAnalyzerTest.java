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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import wherehows.FileAnalyzerFactory;
import wherehows.common.schemas.DatasetJsonRecord;
import wherehows.common.schemas.DatasetSchemaRecord;
import wherehows.common.schemas.SampleDataRecord;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;


/**
 * Created by zsun on 8/18/15.
 */
public class FileAnalyzerTest {
  FileSystem fs;

  @BeforeTest
  public void setUp()
    throws IOException {
    //TODO mockito?
    fs = FileSystem.get(new Configuration()); // set up local file system
  }

  @Test
  public void testAvro()
    throws IOException, URISyntaxException {

    URL url = ClassLoader.getSystemResource("test_sample.avro");
    Path pt = new Path(url.toURI());
    FileAnalyzerFactory fileAnalyzerFactory = new FileAnalyzerFactory(fs);
    DatasetJsonRecord schema = fileAnalyzerFactory.getSchema(pt, "test_sample.avro");

    assert schema != null;
    SampleDataRecord sampleData = fileAnalyzerFactory.getSampleData(pt, "test_sample.avro");
    assert sampleData != null;
  }

  @Test(enabled = false)
  public void testOrc()
    throws IOException, URISyntaxException {
    URL url = ClassLoader.getSystemResource("test_sample.orc");
    Path pt = new Path(url.toURI());
    FileAnalyzerFactory fileAnalyzerFactory = new FileAnalyzerFactory(fs);
    DatasetJsonRecord schema = fileAnalyzerFactory.getSchema(pt, "test_sample.orc");
    assert schema != null;

    SampleDataRecord sampleData = fileAnalyzerFactory.getSampleData(pt, "test_sample.orc");
    assert sampleData != null;
  }
}
