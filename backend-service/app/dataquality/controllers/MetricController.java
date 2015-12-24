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
package dataquality.controllers;

import com.fasterxml.jackson.databind.JsonNode;
import dataquality.DataQualityEngine;
import dataquality.dao.DqMetricDao;
import dataquality.dao.MetricDefDao;
import dataquality.dq.DqMetricResult;
import dataquality.models.MetricDef;
import dataquality.utils.MetricUtil;
import dataquality.utils.QueryBuilder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import play.Logger;
import play.db.DB;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;



/**
 * Created by zechen on 8/3/15.
 */
public class MetricController extends Controller {

  @BodyParser.Of(BodyParser.Json.class)
  public static Result createRule() {
    JsonNode json = request().body().asJson();
    return createRule(json);
  }

  public static Result checkMetric(int metricId) {
    MetricDef metric = DqMetricDao.findById(metricId);
    if (metric != null) {
      return ok(play.api.libs.json.Json.prettyPrint(play.api.libs.json.Json.parse(Json.toJson(MetricUtil.check(metric)).toString())));
    } else {
      Logger.warn("Metric " + metricId + " does not exist");
      return badRequest("Metric " + metricId + " does not exist");
    }
  }


  public static Result checkMetrics(List<Integer> metricIds) {
    List<DqMetricResult> ret = new ArrayList<>();

    for (Integer metricId : metricIds) {
      MetricDef metric = DqMetricDao.findById(metricId);
      if (metric != null) {
        ret.add(MetricUtil.check(metric));
      } else {
        Logger.warn("Metric " + metricId + " does not exist");
        return badRequest("Metric " + metricId + " does not exist");
      }
    }
    return ok(play.api.libs.json.Json.prettyPrint(play.api.libs.json.Json.parse(Json.toJson(ret).toString())));
  }

  public static Result findMetrics(String dataset) {
    List<MetricDef> results = DqMetricDao.findByDataset(dataset);
    return ok(play.api.libs.json.Json.prettyPrint(play.api.libs.json.Json.parse(Json.toJson(results).toString())));
  }

  private static Result createRule(JsonNode json) {
    Result ret;

    try {
      MetricDef metricDef = new MetricDef(json);
      QueryBuilder qb = new QueryBuilder(metricDef);
      String metricDefSql = qb.getInsertMetricDefSql();
      int metricId = MetricDefDao.insertNewRow(metricDefSql);
      metricDef.setId(metricId);
      ret = ok(Json.toJson(metricDef));
    } catch (Exception e) {
      e.printStackTrace();
      ret = internalServerError("Exception: " + e.getMessage());
    }

    return ret;
  }

  public static Result findById(int metricId) {
    MetricDef metricDef = DqMetricDao.findById(metricId);
    JsonNode json = metricDef.getOriginalJson();
    JsonNode userJson = json.findPath("data");
    return ok(play.api.libs.json.Json.prettyPrint(play.api.libs.json.Json.parse(Json.toJson(userJson).toString())));
  }
}
