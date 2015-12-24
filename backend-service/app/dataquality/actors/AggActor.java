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
package dataquality.actors;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import dataquality.DataQualityEngine;
import dataquality.dao.AbstractDao;
import dataquality.dao.AggQueryDao;
import dataquality.dao.AggRunLogDao;
import dataquality.models.AggQuery;
import dataquality.models.enums.JobStatus;
import dataquality.msgs.AggMessage;
import dataquality.msgs.MetricMessage;
import dataquality.utils.QueryBuilder;
import dataquality.utils.TimeUtil;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import play.Logger;


/**
 * Created by zechen on 5/14/15.
 */
public class AggActor extends UntypedActor {
  ActorRef metricActor = DqActorRegistry.metricActor;

  @Override
  public void onReceive(Object message) throws Exception {

    if (message instanceof AggMessage) {
      AggMessage msg = (AggMessage) message;
      Logger.info("Aggregate : " + msg.getAggDefId());

      Set<AggQuery> aggQueries = AggQueryDao.findById(msg.getAggDefId());

      NamedParameterJdbcTemplate dqNamedJdbcTemplate = DataQualityEngine.getInstance().getDataQualityNamedJdbcTemplate();

      // For teradata
      JdbcTemplate tdJdbcTemplate = AbstractDao.tdJdbc;
      Timestamp curTime = new Timestamp(System.currentTimeMillis());
      Timestamp maxPartition = null;
      Timestamp minPartition = null;
      try {
        for (AggQuery aggQuery : aggQueries) {
          String query = aggQuery.getQuery();

          // replace with lower bound time and upper bound time if the query has time dimension constraint
          if (msg.getTimeDimension() != null && msg.getShift() != null) {
            Timestamp lowerBound = TimeUtil.trunc(new Timestamp(System.currentTimeMillis()), msg.getGrain(),
                msg.getShift());
            Timestamp upperBound = TimeUtil.trunc(new Timestamp(System.currentTimeMillis()), msg.getGrain());
            query = QueryBuilder.replace(query, QueryBuilder.LOWER_BOUND, lowerBound.toString());
            query = QueryBuilder.replace(query, QueryBuilder.UPPER_BOUND, upperBound.toString());
          }
          Logger.info("Running aggregation query : " + query);

          AggQueryDao.insertQueryInstance(msg.getAggRunId(),aggQuery.getId(), query);

          List<Map<String, Object>> aggResults = tdJdbcTemplate.queryForList(query);

          String storeQuery = null;
          for (Map<String, Object> aggResult : aggResults) {
            storeQuery = buildQueryTemplate(aggResult.keySet(), aggQuery.getId());
          }


          for (Map<String, Object> aggResult : aggResults) {
            aggResult.put("runId", msg.getAggRunId());
            aggResult.put("timeGrain", msg.getGrain().toString());
            if (msg.getTimeDimension() == null) {
              aggResult.put("dataTime", TimeUtil.trunc(msg.getAggTime(), msg.getGrain()).toString());
            }

            // Capture max and min partition
            if (msg.getTimeDimension() != null) {
              Timestamp ts = (Timestamp) aggResult.get("_data_time");
              if (maxPartition == null || ts.after(maxPartition)) {
                maxPartition = ts;
              }

              if (minPartition == null || ts.before(minPartition)) {
                minPartition = ts;
              }
            }
          }

          if (storeQuery != null) {
            Map<String, Object>[] res = new HashMap[aggResults.size()];
            dqNamedJdbcTemplate.batchUpdate(storeQuery, aggResults.toArray(res));
          }

          if (minPartition != null && maxPartition != null) {
            AggRunLogDao.updatePartitions(maxPartition, minPartition, msg.getAggRunId());
          }
        }

        AggRunLogDao.updateRunStatus(JobStatus.SUCCEEDED, null, msg.getAggRunId());
        metricActor.tell(new MetricMessage(msg.getAggDefId()), getSelf());

      } catch (Exception e) {
        AggRunLogDao.updateRunStatus(JobStatus.ERROR, e.getMessage(), msg.getAggRunId());
        e.printStackTrace();
      }
    }
  }

  private String buildQueryTemplate(Set<String> labels, int tableId) {
    StringBuilder insertPart = new StringBuilder("INSERT INTO " + QueryBuilder.TABLE_PREFIX + tableId
        + " ("+QueryBuilder.RUN_ID + ","  + QueryBuilder.DATA_TIME + "," + QueryBuilder.TIME_GRAIN);
    StringBuilder valuesPart = new StringBuilder(" VALUES (:runId, :dataTime, :timeGrain");
    for (String s : labels) {
      if (s.equals(QueryBuilder.DATA_TIME)) {
        continue;
      }
      insertPart.append(", ");
      insertPart.append(s);
      valuesPart.append(", :");
      valuesPart.append(s);
    }
    insertPart.append(")");
    valuesPart.append(")");

    return insertPart.append(valuesPart).toString();
  }
}
