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

import akka.actor.UntypedActor;
import dataquality.dq.DqMetricResult;
import dataquality.msgs.MetricMessage;
import dataquality.utils.MetricUtil;
import java.util.List;
import play.Logger;
import play.libs.Json;


/**
 * Created by zechen on 4/29/15.
 */
public class MetricActor extends UntypedActor {

  @Override
  public void onReceive(Object message)
      throws Exception {
    if (message instanceof MetricMessage) {
      MetricMessage msg = (MetricMessage) message;
      Logger.info("Checking agg : " + msg.getAggDefId());
      List<DqMetricResult> results = MetricUtil.checkByAggDefId(msg.getAggDefId());
      Logger.info(Json.toJson(results).toString());
      if (results != null && results.size() != 0) {
        MetricUtil.sendNotification(results);
      }
    }
  }
}
