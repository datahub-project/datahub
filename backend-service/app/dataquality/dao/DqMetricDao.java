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
package dataquality.dao;

import com.fasterxml.jackson.databind.JsonNode;
import dataquality.models.MetricDef;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import play.libs.Json;


/**
 * Created by zechen on 8/14/15.
 */
public class DqMetricDao extends AbstractDao {

  public static final String FIND_BY_ID = "select * from dq_metric_def where id = :id";
  public static final String FIND_BY_DATASET =
      "select m.* from dq_metric_def m join dq_agg_def a where m.agg_def_id = a.id and a.dataset_name = :dataset_name";

  public static MetricDef findById(int id) {

    Map<String, Object> params = new HashMap<>();
    params.put("id", id);
    Map<String, Object> resultMap = dqNamedJdbc.queryForMap(FIND_BY_ID, params);

    JsonNode json = Json.parse((String) resultMap.get("metric_def_json"));
    MetricDef metricDef = new MetricDef(json);
    metricDef.setId((Integer) resultMap.get("id"));

    return metricDef;
  }


  public static List<MetricDef> findByDataset(String dataset) {
    List<MetricDef> ret = new LinkedList<>();
    Map<String, Object> params = new HashMap<>();
    params.put("dataset_name", dataset);
    List<Map<String, Object>> resultMaps = dqNamedJdbc.queryForList(FIND_BY_DATASET, params);

    for (Map<String, Object> resultMap : resultMaps) {
      JsonNode json = Json.parse((String) resultMap.get("metric_def_json"));
      MetricDef metricDef = new MetricDef(json);
      metricDef.setId((Integer) resultMap.get("id"));
      ret.add(metricDef);
    }

    return ret;
  }
}
