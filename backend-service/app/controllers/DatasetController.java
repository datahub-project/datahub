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
import java.util.Map;
import models.daos.DatasetDao;
import models.daos.UserDao;
import models.utils.Urn;
import org.springframework.dao.EmptyResultDataAccessException;
import play.Logger;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;


/**
 * Created by zechen on 10/12/15.
 */
public class DatasetController extends Controller {

  public static Result getDatasetWatchers(String datasetName)
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    if (datasetName != null) {
      ObjectNode result = UserDao.getWatchers(datasetName);
      resultJson.put("return_code", 200);
      resultJson.put("watchers", result);
    }
    return ok(resultJson);
  }

  public static Result getDatasetInfo() throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if(datasetIdString != null) {
      int datasetId = Integer.valueOf(datasetIdString);

      try {
        Map<String, Object> dataset = DatasetDao.getDatasetById(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("dataset", Json.toJson(dataset));
      } catch (EmptyResultDataAccessException e) {
        e.printStackTrace();
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset can not find!");
      }
      return ok(resultJson);
    }

    String urn = request().getQueryString("urn");
    if(urn != null) {
      if(!Urn.validateUrn(urn)) {
        resultJson.put("return_code", 400);
        resultJson.put("error_message", "Urn format wrong!");
        return ok(resultJson);
      }
      try {
        Map<String, Object> dataset = DatasetDao.getDatasetByUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("dataset", Json.toJson(dataset));
      } catch (EmptyResultDataAccessException e) {
        e.printStackTrace();
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset can not find!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result addDataset() {
    JsonNode dataset = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetDao.setDatasetRecord(dataset);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset inserted!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result getDatasetDependency() {
    String queryString = request().getQueryString("query");
    JsonNode input = Json.parse(queryString);
    ObjectNode resultJson = Json.newObject();

    try {
      resultJson = DatasetDao.getDatasetDependency(input);
    } catch (Exception e) {
      Logger.error(e.getMessage());
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result getDatasetUrns(String propertiesLike)
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    try {
      if (propertiesLike != null) {
        ObjectNode result = DatasetDao.getDatasetUrnForPropertiesLike(propertiesLike);
        resultJson.put("return_code", 200);
        resultJson.put("dataset_urns", result);
      }
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }
}
