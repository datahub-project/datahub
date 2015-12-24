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
import dataquality.DataQualityEngine;
import dataquality.dao.AggCombDao;
import dataquality.models.AggComb;
import dataquality.models.AggValue;
import dataquality.msgs.AggMessage;
import dataquality.utils.QueryBuilder;
import dataquality.utils.RuleEngine;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.kie.api.runtime.KieSession;
import play.Logger;



/**
 * Created by zechen on 7/7/15.
 */
@Deprecated
public class RuleActor extends UntypedActor {

  @Override
  public void onReceive(Object message) throws Exception {

    AggMessage msg;

    if (message instanceof AggMessage) {
      msg = (AggMessage) message;
    } else {
      return;
    }

    KieSession session = RuleEngine.getInstance().getSession();

    try {
      List<AggComb> aggCombs = AggCombDao.findByAggDefId(msg.getAggDefId());

      for (AggComb aggComb : aggCombs) {
        Integer combId = aggComb.getId();
        String formula = aggComb.getFormula();
        String dimension = aggComb.getDimension();
        String rollupFunc = aggComb.getRollUpFunc();
        Integer queryId = aggComb.getAggQueryId();

        StringBuilder sb = new StringBuilder("");
        sb.append("SELECT create_ts, ");
        if (rollupFunc == null) {
          sb.append(formula);
        } else {
          sb.append(rollupFunc + "(" + formula + ") as " + formula);
        }
        if (dimension != null) {
          sb.append(",");
          sb.append(dimension);
        }
        sb.append(" FROM " + QueryBuilder.TABLE_PREFIX + queryId);
        sb.append(" WHERE create_ts = '" + msg.getAggTime().toString() + "'");
        if (rollupFunc != null && dimension != null) {
          sb.append(" GROUP BY " + dimension);
        }

        String valueQuery = sb.toString();
        Logger.info("value query: " + valueQuery);

        List<Map<String, Object>> values = DataQualityEngine.getInstance().getDataQualityJdbcTemplate().queryForList(valueQuery);

        for (Map<String, Object> value : values) {
          AggValue aggValue = null;
          aggValue = new AggValue(combId, (Timestamp) value.get("create_ts"), (String) value.get(formula), msg.getFrequency());

          if (dimension != null) {
            String[] dims = dimension.split(",");
            StringBuilder dimsStr = new StringBuilder("");
            for (int i = 0; i < dims.length; i++) {
              dimsStr.append((String) value.get(dims[0]));
            }
            aggValue.setDimension(dimsStr.toString());
          }
          session.insert(aggValue);
          Logger.info("Aggregation value: " + aggValue.getValue() + " inserted into rule engine: ");
        }
      }

      session.fireAllRules();
      session.dispose();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
