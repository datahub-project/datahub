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
package wherehows.common.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import wherehows.common.schemas.AzkabanJobExecRecord;


/**
 * Created by zechen on 9/23/15.
 */
public class AzkabanJobExecUtil {

  public static void sortAndSet(List<AzkabanJobExecRecord> records) {
    Collections.sort(records, new Comparator<AzkabanJobExecRecord>() {
      @Override
      public int compare(AzkabanJobExecRecord o1, AzkabanJobExecRecord o2) {
        return o1.getJobName().compareTo(o2.getJobName());
      }
    });

    for (int seq = 0; seq < records.size(); seq++) {
      AzkabanJobExecRecord record = records.get(seq);
      record.setJobExecId(record.getFlowExecId() * 1000L + seq);
    }
  }
}
