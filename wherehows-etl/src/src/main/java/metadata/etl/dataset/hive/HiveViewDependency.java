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
package metadata.etl.dataset.hive;

import java.util.TreeSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.ql.tools.LineageInfo;


/**
 * Created by zsun on 12/14/15.
 */
@Slf4j
public class HiveViewDependency {
  static LineageInfo lineageInfoTool =  new LineageInfo();

  public static String[] getViewDependency(String hiveQl) {
    if (hiveQl == null)
      return new String[]{};

    try {
      lineageInfoTool.getLineageInfo(hiveQl);
      TreeSet<String> inputs = lineageInfoTool.getInputTableList();
      return inputs.toArray(new String[inputs.size()]);
    } catch (Exception e) {
      log.error("Sql statements : \n" + hiveQl + "\n parse ERROR, will return an empty String array");
      log.error(String.valueOf(e.getCause()));
      return new String[]{};
    }
  }
}
