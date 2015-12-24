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
import org.springframework.jdbc.support.KeyHolder;
import play.libs.Json;
import utils.JdbcUtil;


/**
 * Created by zechen on 12/23/15.
 */
public class MetricDefDao extends AbstractDao {
  public static final String METRIC_BY_AGG_DEF_ID = "select * from dq_metric_def where agg_def_id = :aggDefId";


  public static List<MetricDef> findMetricByAggDefId(int aggDefId) {
    Map<String, Object> params = new HashMap<>();
    params.put("aggDefId", aggDefId);
    List<Map<String, Object>> metricDefs = dqNamedJdbc.queryForList(METRIC_BY_AGG_DEF_ID, params);
    List<MetricDef> ret = new LinkedList<>();
    for (Map<String, Object> metricDef : metricDefs) {
      JsonNode json = Json.parse((String) metricDef.get("metric_def_json"));
      MetricDef m = new MetricDef(json);
      m.setId((Integer) metricDef.get("id"));
      ret.add(m);
    }
    return ret;
  }

  public static Integer insertNewRow(String sql) {
    KeyHolder keyHolder = JdbcUtil.insertRow(sql, dqJdbc);
    return keyHolder.getKey().intValue();
  }

}
