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

import dataquality.models.enums.Frequency;
import dataquality.models.enums.TimeGrain;
import dataquality.msgs.AggMessage;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.jdbc.support.KeyHolder;
import utils.JdbcUtil;


/**
 * Created by zechen on 8/18/15.
 */
public class AggDefDao extends AbstractDao {


  private static final String FIND_BY_ID = "select * from dq_agg_def where id = :id";
  private static final String FIND_DATA_TRIGGERED_AGG  = "SELECT distinct d.id, d.dataset_name, d.time_dimension, d.time_grain, d.time_shift, max(r.request_time) last_request_time FROM dq_agg_log r join dq_agg_def d on r.agg_def_id = d.id WHERE frequency = :frequency GROUP BY d.id, d.dataset_name, d.time_dimension, d.time_grain, d.time_shift";
  private static final String FIND_DUE_AGG = " SELECT id, next_run, frequency, time_dimension, time_grain, time_shift FROM dq_agg_def  WHERE next_run <= CURRENT_TIMESTAMP and frequency <> :frequency ";
  private static final String UDPATE_NEXT_RUN = " Update dq_agg_def Set next_run = :nextRun where id = :id";

  public static Map<String, Object> findById(int id) {
    Map<String, Object> params = new HashMap<>();
    params.put("id", id);
    Map<String, Object> resultMap = dqNamedJdbc.queryForMap(FIND_BY_ID, params);
    return resultMap;
  }

  public static Map<String, Set<AggMessage>> getDataTriggeredAggs() {
    Map<String, Object> params = new HashMap<>();
    params.put("frequency", Frequency.DATA_TRIGGERED.toString());
    List<Map<String, Object>> aggs = dqNamedJdbc.queryForList(FIND_DATA_TRIGGERED_AGG, params);
    Map<String, Set<AggMessage>> ret = new HashMap<>();
    for (Map<String, Object> agg : aggs) {
      Integer defId = (Integer) agg.get("id");
      String dataset = (String) agg.get("dataset_name");
      Timestamp lastTime = (Timestamp) agg.get("last_request_time");
      String timeDimension = (String) agg.get("time_dimension");
      TimeGrain timeGrain = TimeGrain.valueOf((String) agg.get("time_grain"));
      Integer timeShift = (Integer) agg.get("time_shift");
      ret.putIfAbsent(dataset, new HashSet<>());
      ret.get(dataset).add(new AggMessage(defId, lastTime, timeDimension, timeGrain, timeShift));
    }

    return ret;
  }


  public static Set<AggMessage> getDueAggs() {
    Map<String, Object> params = new HashMap<>();
    params.put("frequency", Frequency.DATA_TRIGGERED.toString());
    List<Map<String, Object>> aggs = dqNamedJdbc.queryForList(FIND_DUE_AGG, params);
    Set<AggMessage> ret = new HashSet<>();
    for (Map<String, Object> agg : aggs) {
      Integer defId = (Integer) agg.get("id");
      String timeDimension = (String) agg.get("time_dimension");
      TimeGrain timeGrain = TimeGrain.valueOf((String) agg.get("time_grain"));
      Integer timeShift = (Integer) agg.get("time_shift");
      Frequency frequency = Frequency.valueOf((String) agg.get("frequency"));
      ret.add(new AggMessage(defId, new Timestamp(System.currentTimeMillis()), frequency, timeDimension, timeGrain, timeShift));
    }

    return ret;
  }

  public static void updateNextRun(int id, Timestamp nextRun)
      throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("nextRun", nextRun);
    params.put("id", id);
    dqNamedJdbc.update(UDPATE_NEXT_RUN, params);
  }

  public static Integer insertNewRow(String sql) {
    KeyHolder keyHolder = JdbcUtil.insertRow(sql, dqJdbc);
    return keyHolder.getKey().intValue();
  }

}
