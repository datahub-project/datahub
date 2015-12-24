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

import dataquality.models.AggQuery;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.jdbc.support.KeyHolder;
import utils.JdbcUtil;


/**
 * Created by zechen on 12/22/15.
 */
public class AggQueryDao extends AbstractDao {
  private static final String FIND_AGG_QUERY_BY_ID = "SELECT distinct id, generated_query, time_dimension, time_grain FROM dq_agg_query where agg_def_id = :aggDefId";

  public static final String INSERT_QUERY_INSTANCE = "INSERT INTO dq_agg_query_instance(run_id, agg_query_id, query_string) VALUES (:runId, :aggQueryId, :queryString)";


  public static Set<AggQuery> findById(int aggDefId) {
    Map<String, Object> params = new HashMap<>();
    params.put("aggDefId", aggDefId);
    List<Map<String, Object>> queries = dqNamedJdbc.queryForList(FIND_AGG_QUERY_BY_ID, params);
    Set<AggQuery> ret = new HashSet<>();
    for (Map<String, Object> query : queries) {
      Integer queryId = (Integer) query.get("id");
      String queryString = (String) query.get("generated_query");
      ret.add(new AggQuery(queryId, queryString));
    }
    return ret;
  }

  public static void insertQueryInstance(Long runId, Integer aggQueryId, String queryString) {
    Map<String, Object> params = new HashMap<>();
    params.put("runId", runId);
    params.put("aggQueryId", aggQueryId);
    params.put("queryString", queryString);
    dqNamedJdbc.update(INSERT_QUERY_INSTANCE, params);
  }

  public static Integer insertNewRow(String sql) {
    KeyHolder keyHolder = JdbcUtil.insertRow(sql, dqJdbc);
    return keyHolder.getKey().intValue();
  }

}
