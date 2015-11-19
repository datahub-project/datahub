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
package metadata.etl;

/**
 * Created by zsun on 9/23/15.
 */

import metadata.etl.lineage.AzLineageMetadataEtl;


/**
 * For a standalone lineage ETL entrance
 * Created by zsun on 9/19/15.
 */
public class Launcher {
  public static void main(String[] args)
    throws Exception {
    String type = args[0];
    AzLineageMetadataEtl lm = null;
    switch (type) {
      case "dev":
        lm = new AzLineageMetadataEtl(31);
        break;

      case "prod":
        lm = new AzLineageMetadataEtl(32);
        break;

      default:
        System.err.println("Pass a paramter of 'dev' or 'prod'");
        break;
    }
    assert lm != null;

    if (args.length > 1) {
      lm.timeFrame = Integer.valueOf(args[1]);
    }
    lm.run();
  }
}
