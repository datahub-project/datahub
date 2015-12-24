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

import akka.actor.ActorRef;
import akka.actor.Props;
import com.fasterxml.jackson.databind.JsonNode;
import dataquality.actors.AggActor;
import dataquality.dao.AbstractDao;
import dataquality.dao.AggCombDao;
import dataquality.dao.AggDefDao;
import dataquality.dao.AggQueryDao;
import dataquality.models.AggComb;
import dataquality.models.AggDef;
import dataquality.models.Formula;
import dataquality.models.enums.TimeGrain;
import dataquality.msgs.AggMessage;
import dataquality.utils.QueryBuilder;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import play.Logger;
import play.api.mvc.Action;
import play.api.mvc.AnyContent;
import play.libs.Akka;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;


public class AggregationController extends Controller {
  public static Action<AnyContent> index(String all) {
    return controllers.Assets.at("/public/html", "agg.html");
  }

  public static Result checkPreFlight(String all) {
    response().setHeader("Access-Control-Allow-Origin", "*");       // Need to add the correct domain in here!!
    response().setHeader("Access-Control-Allow-Methods", "POST");   // Only allow POST
    response().setHeader("Access-Control-Max-Age", "300");          // Cache response for 5 minutes
    response().setHeader("Access-Control-Allow-Headers",
        "Origin, X-Requested-With, Content-Type, Accept");         // Ensure this header is also allowed!
    return ok();
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result previewAggregation() {
    JsonNode json = request().body().asJson();
    return previewAggregation(json);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result createAggregation() {
    JsonNode json = request().body().asJson();
    return createAggregation(json);
  }

  private static Result previewAggregation(JsonNode json) {
    Result ret = null;
    try {
      // Validate json and transform it into a AggDef object
      AggDef aggDef = new AggDef(json);
      QueryBuilder qb = new QueryBuilder(aggDef);
      List<String> queries = qb.preview();
      ret = ok(Json.toJson(queries));
    } catch (IllegalArgumentException ilaE) {
      ilaE.printStackTrace();
      ret = badRequest("Illegal argument exception: " + ilaE.getMessage());
    }
    return ret;
  }

  private static Result createAggregation(JsonNode json) {
    Result ret;
    try {

      AggDef aggDef = new AggDef(json);
      QueryBuilder qb = new QueryBuilder(aggDef);
      // create aggregation definitions
      String defSql = qb.getInsertAggDefSql();
      Logger.info("agg definition insert : " + defSql);
      int defId = AggDefDao.insertNewRow(defSql);

      // create aggregation queries

      List<Formula> allFormulas = aggDef.getFormulas();
      // have dimension columns
      if (aggDef.getIndDimensions() != null && !aggDef.getIndDimensions().isEmpty()) {
        boolean overallAdded = false;

        for (List<String> dims : aggDef.getIndDimensions().keySet()) {
          String querySql = qb.getInsertAggQuerySql(defId, allFormulas, dims);
          int queryId = AggQueryDao.insertNewRow(querySql);

          // create ddl for storing the data
          String createSql = qb.getCreateAggStoreSql(queryId, allFormulas, dims);
          AbstractDao.dqJdbc.execute(createSql);
          for (Formula f : allFormulas) {
            String combSql = qb.getInsertAggCombSql(defId, queryId, f, dims, false);
            AbstractDao.dqJdbc.batchUpdate(combSql);
            // add roll up combinations
            if (f.isAdditive()) {
              for (List<String> child : aggDef.getIndDimensions().get(dims)) {
                AbstractDao.dqJdbc.batchUpdate(qb.getInsertAggCombSql(defId, queryId, f, child, true));
              }

              if (!overallAdded && f.isOverall()) {
                AbstractDao.dqJdbc.batchUpdate(qb.getInsertAggCombSql(defId, queryId, f, null, true));
              }
            }
          }
          overallAdded = true;

          // Add non additive formulas for all roll up dimensions
          List<Formula> nonAdtFormulas = aggDef.getNonAdtFormulas();

          if (nonAdtFormulas != null && !nonAdtFormulas.isEmpty()) {

            for (List<String> child : aggDef.getIndDimensions().get(dims)) {
              int childQueryId = AggQueryDao.insertNewRow(qb.getInsertAggQuerySql(defId, nonAdtFormulas, child));
              AbstractDao.dqJdbc.execute(qb.getCreateAggStoreSql(childQueryId, nonAdtFormulas, child));
              for (Formula nf : nonAdtFormulas) {
                AbstractDao.dqJdbc.batchUpdate(qb.getInsertAggCombSql(defId, childQueryId, nf, child, false));
              }
            }
          }
        }

        List<Formula> nonAdtOverallFormulas = aggDef.getNonAdtOverallFormulas();
        if (nonAdtOverallFormulas != null && !nonAdtOverallFormulas.isEmpty()) {
          int queryId = AggQueryDao.insertNewRow(qb.getInsertAggQuerySql(defId, nonAdtOverallFormulas, null));
          AbstractDao.dqJdbc.execute(qb.getCreateAggStoreSql(queryId, nonAdtOverallFormulas, null));
          for (Formula f : nonAdtOverallFormulas) {
            AbstractDao.dqJdbc.batchUpdate(qb.getInsertAggCombSql(defId, queryId, f, null, false));
          }
        }
      } else {
        // no dimension specified, need add all formulas
        int queryId = AggQueryDao.insertNewRow(qb.getInsertAggQuerySql(defId, allFormulas, null));
        AbstractDao.dqJdbc.execute(qb.getCreateAggStoreSql(queryId, allFormulas, null));
        for (Formula f : allFormulas) {
          AbstractDao.dqJdbc.batchUpdate(qb.getInsertAggCombSql(defId, queryId, f, null, false));
        }
      }

      List<AggComb> combs = AggCombDao.findByAggDefId(defId);
      ret = ok(Json.toJson(combs));
    } catch (Exception e) {
      e.printStackTrace();
      ret = internalServerError("Exception: " + e.getMessage());
    }

    return ret;
  }

  public static Result runAggregation(int aggDefId) {
    Map<String, Object> aggMap = AggDefDao.findById(aggDefId);
    ActorRef aggActor = Akka.system().actorOf(Props.create(AggActor.class));
    String timeDimension = (String) aggMap.get("time_dimension");
    TimeGrain timeGrain = TimeGrain.valueOf((String) aggMap.get("time_grain"));
    Integer timeShift = (Integer) aggMap.get("time_shift");
    aggActor
        .tell(new AggMessage(aggDefId, new Timestamp(System.currentTimeMillis()), timeDimension, timeGrain, timeShift),
            ActorRef.noSender());
    return ok("Aggregation manually triggered");
  }

  public static Result findById(int aggDefId) {
    Map<String, Object> aggMap = AggDefDao.findById(aggDefId);
    JsonNode json = Json.parse((String) aggMap.get("agg_def_json"));
    JsonNode userJson = json.findPath("data");
    return ok(play.api.libs.json.Json.prettyPrint(play.api.libs.json.Json.parse(Json.toJson(userJson).toString())));
  }

  public static Result findCombsByAggDefId(int aggDefId) {
    List<AggComb> combs = AggCombDao.findByAggDefId(aggDefId);
    return ok(play.api.libs.json.Json.prettyPrint(play.api.libs.json.Json.parse(Json.toJson(combs).toString())));
  }

  public static Result findCombById(int id) {
    AggComb comb = AggCombDao.findById(id);
    return ok(play.api.libs.json.Json.prettyPrint(play.api.libs.json.Json.parse(Json.toJson(comb).toString())));
  }
}
