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
package metadata.etl.dataset.druid;

import org.junit.Test;

public class DruidMetadataExtractorTest {
    public DruidMetadataExtractorTest(){

    }

    @Test
    public void extractorTest() throws Exception{
        String url = "http://xvac-g13.xv.dc.openx.org:8080/druid/v2";
        String schema_csv_file = "/Users/wenhua.wang/druid_schema.csv";
        String field_csv_file = "/Users/wenhua.wang/druid_field.csv";
        DruidMetadataExtractor dme = new DruidMetadataExtractor(url, schema_csv_file, field_csv_file);
        dme.run();
    }
}
