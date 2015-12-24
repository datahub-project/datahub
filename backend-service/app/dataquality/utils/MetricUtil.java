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
package dataquality.utils;

import dataquality.DataQualityEngine;
import dataquality.dao.AggDefDao;
import dataquality.dao.AggRunLogDao;
import dataquality.dao.DqMetricDao;
import dataquality.dao.MetricDefDao;
import dataquality.dq.DqFunction;
import dataquality.dq.DqFunctionFactory;
import dataquality.dq.DqMetricResult;
import dataquality.dq.DqRule;
import dataquality.dq.DqRuleResult;
import dataquality.dq.DqStatus;
import dataquality.models.AggComb;
import dataquality.models.MetricDef;
import dataquality.models.MetricValue;
import dataquality.models.TimeRange;
import dataquality.models.enums.TimeGrain;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.mail.MessagingException;
import play.Logger;
import play.libs.Json;


/**
 * Created by zechen on 8/6/15.
 */
public class MetricUtil {
  public static DqMetricResult check(int metricId) {

    MetricDef metric = DqMetricDao.findById(metricId);
    if (metric != null) {
      return check(metric);
    } else {
      Logger.debug("Metric " + metricId + " does not exist");
      return null;
    }
  }

  public static List<DqMetricResult> checkByAggDefId(int aggDefId) {
    List<MetricDef> metrics = MetricDefDao.findMetricByAggDefId(aggDefId);
    List<DqMetricResult> ret = new ArrayList<>();
    for (MetricDef metric : metrics) {
      ret.add(check(metric));
    }

    return ret;
  }

  public static DqMetricResult check(MetricDef metric) {
    DqMetricResult ret = new DqMetricResult();
    QueryBuilder qb = new QueryBuilder(metric);
    List<DqRule> rules = metric.getRules();

    List<TimeRange> lastTwoRun = AggRunLogDao.getLastTwoRuns(metric.getAggDefId());
    List<DqRuleResult> ruleResults = new ArrayList<>();
    for (DqRule dqRule : rules) {
      DqFunction dqFunction = DqFunctionFactory.newFunction(dqRule.getFunc(), dqRule.getParams());
      List<TimeRange> ranges = dqFunction.getTimeRanges(lastTwoRun);
      if (ranges == null || ranges.size() == 0) {
        ruleResults.add(new DqRuleResult(DqStatus.NO_ENOUGH_DATA));
      } else {
        qb.clearTimeRanges();
        qb.add(ranges);
        Logger.debug("Metric value sql: " + qb.getSql());
        List<MetricValue> values = findMetricValue(qb.getSql(), metric.getComb());
        ruleResults.add(dqFunction.computeAndValidate(values, dqRule.getCriteria()));
      }
    }
    ret.setMetricId(metric.getId());
    ret.setAggDefId(metric.getAggDefId());
    ret.setMetricName(metric.getName());
    ret.setRuleResults(ruleResults);
    Logger.debug(Json.toJson(ret).toString());
    return ret;
  }

  public static List<MetricValue> findMetricValue(String sql, AggComb comb) {
    List<MetricValue> ret = new ArrayList<>();

    List<Map<String, Object>> values = DataQualityEngine.getInstance().getDataQualityJdbcTemplate().queryForList(sql);

    String dimension = comb.getDimension();

    for (Map<String, Object> value : values) {
      MetricValue metricValue = new MetricValue((Timestamp) value.get(QueryBuilder.DATA_TIME),
          TimeGrain.valueOf((String) value.get(QueryBuilder.TIME_GRAIN)), Double.valueOf(value.get(QueryBuilder.DATA_VALUE).toString()));
      if (dimension != null) {
        String[] dims = dimension.split(",");
        StringBuilder sb = new StringBuilder("");
        for (String dim : dims) {
          sb.append(value.get(dim) + ",");
        }
        metricValue.setDimension(sb.toString().substring(0, sb.length() - 1));
      }
      ret.add(metricValue);
    }

    return ret;
  }

  public static void sendNotification(List<DqMetricResult> results)
      throws Exception {
    DqStatus summary = DqStatus.PASSED;
    int cntFailed = 0, cntNoEnoughData = 0, cntPassed = 0;
    int aggDefId = 0;
    for (DqMetricResult r : results) {
      aggDefId = r.getAggDefId();
      for (DqRuleResult ruleResult : r.getRuleResults()) {
        switch (ruleResult.getStatus()) {
          case PASSED:
            cntPassed++;
            break;
          case FAILED:
            cntFailed++;
            break;
          case NO_ENOUGH_DATA:
            cntNoEnoughData++;
            break;
        }
      }
    }

    if (cntFailed > 0) {
      summary = DqStatus.FAILED;
    } else if (cntNoEnoughData > 0) {
      summary = DqStatus.NO_ENOUGH_DATA;
    }

    Map<String, Object> aggDef = AggDefDao.findById(aggDefId);
    String subject =
        "[" + summary.name() + "] Data quality result for aggregation id " + aggDefId + " dataset " + aggDef
            .get("dataset_name");
    String recipients = (String) aggDef.get("owner");
    String content = Json.toJson(results).toString();
    String prettyContent =
        "<html><body><pre class=\"prettyprint\"><code class=\"language-json\">" + play.api.libs.json.Json
            .prettyPrint(play.api.libs.json.Json.parse(content)) + "</code></pre></body></html>";
    EmailService.sendMail(recipients, subject, prettyContent);
  }
}
