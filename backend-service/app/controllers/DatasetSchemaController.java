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
import models.daos.DatasetSchemaDao;
import models.utils.Urn;
import org.springframework.dao.EmptyResultDataAccessException;
import play.Logger;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;


public class DatasetSchemaController extends Controller {

  public static Result getDatasetConstraint()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        List<Map<String, Object>> constraints = DatasetSchemaDao.getDatasetConstraintByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("dataset_constraints", Json.toJson(constraints));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " constraints cannot be found!");
      }
      return ok(resultJson);
    }

    String urn = request().getQueryString("urn");
    if (urn != null) {
      if (!Urn.validateUrn(urn)) {
        resultJson.put("return_code", 400);
        resultJson.put("error_message", "Urn format wrong!");
        return ok(resultJson);
      }
      try {
        List<Map<String, Object>> constraints = DatasetSchemaDao.getDatasetConstraintByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("dataset_constraints", Json.toJson(constraints));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " constraints cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatesetConstraint() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetSchemaDao.updateDatasetConstraint(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset constraints updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetIndex()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        List<Map<String, Object>> indices = DatasetSchemaDao.getDatasetIndexByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("dataset_indices", Json.toJson(indices));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " indices cannot be found!");
      }
      return ok(resultJson);
    }

    String urn = request().getQueryString("urn");
    if (urn != null) {
      if (!Urn.validateUrn(urn)) {
        resultJson.put("return_code", 400);
        resultJson.put("error_message", "Urn format wrong!");
        return ok(resultJson);
      }
      try {
        List<Map<String, Object>> indices = DatasetSchemaDao.getDatasetIndexByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("dataset_indices", Json.toJson(indices));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " indices cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatesetIndex() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetSchemaDao.updateDatasetIndex(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset indices updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }


  public static Result getDatasetSchema()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        Map<String, Object> schema = DatasetSchemaDao.getDatasetSchemaByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("dataset_schema", Json.toJson(schema));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " schema cannot be found!");
      }
      return ok(resultJson);
    }

    String urn = request().getQueryString("urn");
    if (urn != null) {
      if (!Urn.validateUrn(urn)) {
        resultJson.put("return_code", 400);
        resultJson.put("error_message", "Urn format wrong!");
        return ok(resultJson);
      }
      try {
        Map<String, Object> schema = DatasetSchemaDao.getDatasetSchemaByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("dataset_schema", Json.toJson(schema));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " schema cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatesetSchema() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetSchemaDao.updateDatasetSchema(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset schema updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }
}
