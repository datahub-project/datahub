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
import dataquality.dao.AggDefDao;
import dataquality.dao.AggRunLogDao;
import dataquality.msgs.AggMessage;
import dataquality.utils.TimeUtil;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Set;
import models.daos.LineageDao;
import play.Logger;


/**
 * Responsible for processing periodically message from the scheduler
 *
 * Created by zechen on 4/24/15.
 */
public class DqSchedulerActor extends UntypedActor {
  public static final String MESSAGE = "checking-dq";
  public static final Long ONE_DAY = 24 * 60 * 60L;
  ActorRef aggActor = DqActorRegistry.aggActor;

  @Override
  public void onReceive(Object message)
      throws Exception {
    String msg = null;

    if (MESSAGE.equals(message)) {

      Logger.info("Checking data trigger aggregation");
      Map<String, Long> datasets = LineageDao.getRecentUpdatedDataset(System.currentTimeMillis() / 1000 - ONE_DAY);
      Map<String, Set<AggMessage>> datasetMsg = AggDefDao.getDataTriggeredAggs();
      for (String dataset : datasets.keySet()) {
        if (datasetMsg.containsKey(dataset)) {
          Timestamp latest = new Timestamp(datasets.get(dataset) * 1000);
          for (AggMessage aggMessage : datasetMsg.get(dataset)) {
            if (aggMessage.getAggTime() == null || aggMessage.getAggTime().before(latest)) {
              Logger.info("New data available for def id: " + aggMessage.getAggDefId() + " at " + latest + "(Old time: "
                  + aggMessage.getAggTime() + ")");
              aggMessage.setAggTime(latest);
              requestAggregation(aggMessage);
            }
          }
        }
      }

      Logger.info("Checking periodic aggregation");
      Set<AggMessage> dueAggs = AggDefDao.getDueAggs();
      Logger.info("{} aggregations are due.", dueAggs.size());
      for (AggMessage dueAgg : dueAggs) {
        requestAggregation(dueAgg);
        AggDefDao.updateNextRun(dueAgg.getAggDefId(),
            TimeUtil.getNextRun(new Timestamp(System.currentTimeMillis()), dueAgg.getFrequency()));
      }
    }
  }

  private void requestAggregation(AggMessage aggMessage) {
    Long runId = AggRunLogDao.insertNewAggRunLog(aggMessage.getAggDefId(), new Timestamp(System.currentTimeMillis()), TimeUtil.trunc(aggMessage.getAggTime(), aggMessage.getGrain()));
    aggMessage.setAggRunId(runId);
    aggActor.tell(aggMessage, getSelf());
  }


}
