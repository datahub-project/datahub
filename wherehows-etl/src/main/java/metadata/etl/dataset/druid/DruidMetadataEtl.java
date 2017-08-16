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

import java.io.InputStream;
import java.util.Properties;
import metadata.etl.EtlJob;

public class DruidMetadataEtl extends EtlJob{

    public DruidMetadataEtl(int dbId, long whExecId, Properties prop) {
        super(null, dbId, whExecId, prop);
    }

    @Override
    public void extract()
            throws Exception {
        logger.info("Running Druid metadata Extractor");
//        InputStream inputStream = classLoader.getResourceAsStream(("jython/DruidExtract.py"));
//        logger.info("call scripts with args: "+interpreter.getSystemState().argv);
//        interpreter.execfile(inputStream);
//        inputStream.close();
        DruidMetadataExtractor extractor = new DruidMetadataExtractor(prop);
        extractor.run();
    }

    @Override
    public void transform() throws Exception {
        // TODO Auto-generated method stub
        logger.info("Running Druid Trasform!");

    }
    @Override
    public void load() throws Exception {
        logger.info("Running Druid Load");
//        logger.info("In Vertica metadata ETL, launch load jython scripts");
//        InputStream inputStream = classLoader.getResourceAsStream("jython/DruidLoad.py");
//        interpreter.execfile(inputStream);
//        inputStream.close();
        DruidMetadataLoader loader = new DruidMetadataLoader(prop);
        loader.run();
    }
}
