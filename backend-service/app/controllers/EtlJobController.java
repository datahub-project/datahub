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
import models.daos.EtlJobDao;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import java.util.List;
import java.util.Map;


/**
 * Created by zechen on 10/21/15.
 */
public class EtlJobController extends Controller {

  public static Result getAllJobs() {
    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> jobs = EtlJobDao.getAllJobs();
      resultJson.put("return_code", 200);
      resultJson.set("etl_jobs", Json.toJson(jobs));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result getEtlJobById(int id) {

    ObjectNode resultJson = Json.newObject();
    try {
      Map<String, Object> etlJob = EtlJobDao.getEtlJobById(id);
      resultJson.put("return_code", 200);
      resultJson.set("etl_job", Json.toJson(etlJob));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result addEtlJob() {
    JsonNode etlJob = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      int key = EtlJobDao.insertEtlJob(etlJob);
      resultJson.put("return_code", 200);
      resultJson.put("generated_key", key);
      resultJson.put("message", "Etl job created!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result updateEtlJobStatus() {
    JsonNode jobStatus = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      EtlJobDao.updateJobStatus(jobStatus);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Etl job status updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(Json.toJson(resultJson));
  }

  public static Result updateEtlJobSchedule() {
    JsonNode jobSchedule = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      EtlJobDao.updateJobSchedule(jobSchedule);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Etl job schedule updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(Json.toJson(resultJson));
  }
}
