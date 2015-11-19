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
import models.daos.PatternDao;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import java.util.List;
import java.util.Map;

/**
 * Created by zechen on 10/30/15.
 */
public class PatternController extends Controller {

  public static Result getAllFilenamePatterns() {
    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> fileNamePatterns = PatternDao.getAllFileNamePatterns();
      resultJson.put("return_code", 200);
      resultJson.set("filename_patterns", Json.toJson(fileNamePatterns));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }


  public static Result addFilenamePattern() {
    JsonNode filename = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      int key = PatternDao.insertFilenamePattern(filename);
      resultJson.put("return_code", 200);
      resultJson.put("generated_key", key);
      resultJson.put("message", "New filename pattern created!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }


  public static Result getAllDatasetPartitionPatterns() {
    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> datasetPartitionPatterns = PatternDao.getAllDatasetPartitionPatterns();
      resultJson.put("return_code", 200);
      resultJson.set("dataset_partition_patterns", Json.toJson(datasetPartitionPatterns));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }


  public static Result addDatasetPartitionPattern() {
    JsonNode datasetPattern = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      int key = PatternDao.insertDatasetPartitionPattern(datasetPattern);
      resultJson.put("return_code", 200);
      resultJson.put("generated_key", key);
      resultJson.put("message", "New dataset partition pattern created!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }


  public static Result getAllLineagePattern() {
    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> lineagePatterns = PatternDao.getAllLogLineagePattern();
      resultJson.put("return_code", 200);
      resultJson.set("lineage_patterns", Json.toJson(lineagePatterns));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }


  public static Result addLineagePattern() {
    JsonNode lineagePattern = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      int key = PatternDao.insertLogLineagePattern(lineagePattern);
      resultJson.put("return_code", 200);
      resultJson.put("generated_key", key);
      resultJson.put("message", "New lineage pattern created!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result getAllJobIdPattern() {
    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> jobIdPatterns = PatternDao.getAllLogJobIdPattern();
      resultJson.put("return_code", 200);
      resultJson.set("job_id_patterns", Json.toJson(jobIdPatterns));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }


  public static Result addJobIdPattern() {
    JsonNode jobIdPattern = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      int key = PatternDao.insertLogJobIdPattern(jobIdPattern);
      resultJson.put("return_code", 200);
      resultJson.put("generated_key", key);
      resultJson.put("message", "New job id pattern created!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }




}
