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
package controllers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import models.daos.LineageDao;
import utils.Urn;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;


/**
 * Created by zsun on 4/5/15.
 */
public class LineageController extends Controller {
  public static Result getJobsByDataset(String urn) throws SQLException {
    if (!Urn.validateUrn(urn)) {
      ObjectNode resultJson = Json.newObject();
      resultJson.put("return_code", 400);
      resultJson.put("error_message", "Urn format wrong!");
      return ok(resultJson);
    }

    String period = request().getQueryString("period");
    if (period == null) {
      period = "30"; // default look at recent 30 days
    }

    String cluster = request().getQueryString("cluster");
    String instance = request().getQueryString("instance");
    String direction = request().getQueryString("direction");
    String sourceTargetType = "source";
    if (direction != null && direction.toLowerCase().equals("upstream")) {
      sourceTargetType = "target";
    }
    ObjectNode resultJson = Json.newObject();

    try {
      List<Map<String, Object>> jobs = LineageDao.getJobsByDataset(urn, period, cluster, instance, sourceTargetType);
      resultJson.put("return_code", 200);
      resultJson.set("jobs", Json.toJson(jobs));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetsByJob(String flowPath, String jobName) {
    String instance = request().getQueryString("instance");
    String direction = request().getQueryString("direction");
    String sourceTargetType = "target";
    if (direction != null && direction.toLowerCase().equals("upstream")) {
      sourceTargetType = "source";
    }

    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> datasets = LineageDao.getDatasetsByJob(flowPath, jobName, instance, sourceTargetType);
      resultJson.put("return_code", 200);
      resultJson.set("datasets", Json.toJson(datasets));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetsByFlowExec(Long flowExecId, String jobName) {
    String instance = request().getQueryString("instance");
    String direction = request().getQueryString("direction");
    String sourceTargetType = "target";
    if (direction != null && direction.toLowerCase().equals("upstream")) {
      sourceTargetType = "source";
    }

    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> datasets = LineageDao.getDatasetsByFlowExec(flowExecId, jobName, instance, sourceTargetType);
      resultJson.put("return_code", 200);
      resultJson.set("datasets", Json.toJson(datasets));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetsByJobExec(Long jobExecId) {
    String instance = request().getQueryString("instance");
    String direction = request().getQueryString("direction");
    String sourceTargetType = "target";
    if (direction != null && direction.toLowerCase().equals("upstream")) {
      sourceTargetType = "source";
    }

    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> datasets = LineageDao.getDatasetsByJobExec(jobExecId, instance, sourceTargetType);
      resultJson.put("return_code", 200);
      resultJson.set("datasets", Json.toJson(datasets));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result addJobLineage() {
    JsonNode lineage = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      LineageDao.insertLineage(lineage);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Lineage inserted!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateJobExecutionLineage() {
    JsonNode lineage = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      LineageDao.updateJobExecutionLineage(lineage);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Job Execution Lineage Updated!");
    } catch (Exception e) {
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }
}
