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
package models.daos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import utils.JdbcUtil;
import wherehows.common.schemas.DatasetRecord;
import wherehows.common.utils.PartitionPatternMatcher;
import wherehows.common.writers.DatabaseWriter;


/**
 * Created by zsun on 3/18/15.
 * Modified by zechen on 10/12/15.
 */
public class DatasetDao {

  public static final String GET_DATASET_BY_ID = "SELECT * FROM dict_dataset WHERE id = :id";
  public static final String GET_DATASET_BY_URN = "SELECT * FROM dict_dataset WHERE urn = :urn";

  public static Map<String, Object> getDatasetById(int datasetId)
    throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put("id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_BY_ID, params);
  }

  public static Map<String, Object> getDatasetByUrn(String urn)
    throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put("urn", urn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_BY_URN, params);
  }

  public static void insertDataset(JsonNode dataset)
    throws Exception {

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    DatasetRecord record = om.convertValue(dataset, DatasetRecord.class);
    Map<String, Object> refDataset = getDatasetByUrn(record.getRefDatasetUrn());

    // Find ref dataset id
    if (refDataset != null) {
      record.setRefDatasetId((int) refDataset.get("id"));
    }

    // Find layout id
    if (record.getSamplePartitionFullPath() != null) {
      PartitionPatternMatcher ppm = new PartitionPatternMatcher(PartitionLayoutDao.getPartitionLayouts());
      record.setPartitionLayoutPatternId(ppm.analyze(record.getSamplePartitionFullPath()));
    }

    DatabaseWriter dw = new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, "dict_dataset");
    dw.append(record);
    dw.close();
  }
}
