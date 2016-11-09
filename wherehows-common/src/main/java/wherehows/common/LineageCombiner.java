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
package wherehows.common;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import wherehows.common.schemas.LineageRecord;


/**
 * There are situations we find a lot of input at file level or one data set's different partitions.
 * We need to combine those lineage records that with same abstract path together.
 * e.g. input is /data/somedataset/2015/01/01/part1, /data/somedataset/2015/01/02/part1, /data/somedataset/2015/01/03/part1,
 * Then we just want a output of dataset of /data/somedataset with a range from 2015/01/01 to 2015/01/03
 * Created by zsun on 9/11/15.
 */
public class LineageCombiner {

  // key is the operation + abstract name, value is the record
  private Map<String, LineageRecord> _lineageRecordMap;

  public LineageCombiner(Connection connection) {
    _lineageRecordMap = new HashMap<>();
  }

  /**
   * Add all raw record into combiner, the combiner will reduce the records by operation and abstract dataset name.
   * Assume the dataset of input lineage would came from a same cluster.
   * @param rawLineageRecords
   */
  public void addAll(List<LineageRecord> rawLineageRecords) {
    for (LineageRecord lr : rawLineageRecords) {
      DatasetPath datasetPath = PathAnalyzer.analyze(lr.getFullObjectName());
      if (datasetPath != null) {
        lr.updateDataset(datasetPath);
        addToMap(lr);
      }
    }
  }

  /**
   * Similar to addAll but not update partition info if exist
   * @param rawLineageRecords
   */
  public void addAllWoPartitionUpdate(List<LineageRecord> rawLineageRecords) {
    for (LineageRecord lr : rawLineageRecords) {
      DatasetPath datasetPath = PathAnalyzer.analyze(lr.getFullObjectName());
      if (datasetPath != null) {
        lr.setAbstractObjectName(datasetPath.abstractPath);
        lr.setLayoutId(datasetPath.layoutId);
        if (lr.getPartitionStart() == null) {
          lr.setPartitionStart(datasetPath.partitionStart);
        }
        if (lr.getPartitionEnd() == null) {
          lr.setPartitionEnd(datasetPath.partitionEnd);
        }
        if (lr.getPartitionType() == null) {
          lr.setPartitionType(datasetPath.partitionType);
        }
        addToMap(lr);
      }
    }
  }

  private void addToMap(LineageRecord lr) {
    String lineageRecordKey = lr.getLineageRecordKey();
    if (_lineageRecordMap.containsKey(lineageRecordKey)) {
      // merge
      _lineageRecordMap.get(lineageRecordKey).merge(lr);
    } else {
      _lineageRecordMap.put(lineageRecordKey, lr);
    }
  }

  /**
   * Get all the lineage from the map, sort and give them the srl_no
   * @return A list of {@code LineageRecord} after combined.
   */
  public List<LineageRecord> getCombinedLineage() {
    List<LineageRecord> allLineage = new ArrayList<>(_lineageRecordMap.values());
    Collections.sort(allLineage);

    for (int i = 0; i < allLineage.size(); i++) {
      allLineage.get(i).setSrlNo(i);
    }

    return allLineage;
  }
}
