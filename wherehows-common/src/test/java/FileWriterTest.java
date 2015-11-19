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
import java.sql.SQLException;
import org.testng.Assert;
import wherehows.common.schemas.Record;
import wherehows.common.schemas.SampleDataRecord;
import wherehows.common.writers.FileWriter;
import wherehows.common.writers.Writer;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by zsun on 8/20/15.
 */
@Test(groups = {"wherehows.common"})
public class FileWriterTest {

  @Test
  public void writeTest()
    throws IOException, SQLException {
    String filePath = "testFile.txt";
    Writer w = new FileWriter(filePath);
    List<Object> sample = new ArrayList<Object>();
    sample.add("aaa");
    sample.add("bbb");
    sample.add("ccc");
    Record record = new SampleDataRecord("/a/b/c", sample);
    System.out.println(record.toCsvString());
    w.append(record);
    w.append(record);
    w.close();

    BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
    String line1 = bufferedReader.readLine();
    String line2 = bufferedReader.readLine();

    Assert.assertEquals(record.toCsvString().trim(), line1);
    Assert.assertEquals(record.toCsvString().trim(), line2);

    bufferedReader.close();
  }
}
