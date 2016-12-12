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
import models.daos.DatasetInfoDao;
import utils.Urn;
import org.springframework.dao.EmptyResultDataAccessException;
import play.Logger;
import play.libs.Json;
import play.mvc.BodyParser;
import play.mvc.Controller;
import play.mvc.Result;
import wherehows.common.schemas.DatasetCapacityRecord;
import wherehows.common.schemas.DatasetCaseSensitiveRecord;
import wherehows.common.schemas.DatasetComplianceRecord;
import wherehows.common.schemas.DatasetConstraintRecord;
import wherehows.common.schemas.DatasetIndexRecord;
import wherehows.common.schemas.DatasetOwnerRecord;
import wherehows.common.schemas.DatasetPartitionRecord;
import wherehows.common.schemas.DatasetReferenceRecord;
import wherehows.common.schemas.DatasetSchemaInfoRecord;
import wherehows.common.schemas.DatasetSecurityRecord;
import wherehows.common.schemas.DatasetTagRecord;
import wherehows.common.schemas.DeploymentRecord;


public class DatasetInfoController extends Controller {

  public static Result getDatasetDeployment()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        List<DeploymentRecord> records = DatasetInfoDao.getDatasetDeploymentByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("deploymentInfo", Json.toJson(records));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " deployment info cannot be found!");
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
        List<DeploymentRecord> records = DatasetInfoDao.getDatasetDeploymentByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("deploymentInfo", Json.toJson(records));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " deployment info cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatesetDeployment() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetDeployment(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset deployment updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetCapacity()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        List<DatasetCapacityRecord> records = DatasetInfoDao.getDatasetCapacityByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("capacity", Json.toJson(records));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " capacity info cannot be found!");
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
        List<DatasetCapacityRecord> records = DatasetInfoDao.getDatasetCapacityByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("capacity", Json.toJson(records));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " capacity info cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatesetCapacity() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetCapacity(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset capacity updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetTags()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        List<DatasetTagRecord> records = DatasetInfoDao.getDatasetTagByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("tags", Json.toJson(records));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " tags cannot be found!");
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
        List<DatasetTagRecord> records = DatasetInfoDao.getDatasetTagByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("tags", Json.toJson(records));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " tags info cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatesetTags() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetTags(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset tags updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetCaseSensitivity()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        DatasetCaseSensitiveRecord record = DatasetInfoDao.getDatasetCaseSensitivityByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("caseSensitivity", Json.toJson(record));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " case_sensitive info cannot be found!");
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
        DatasetCaseSensitiveRecord record = DatasetInfoDao.getDatasetCaseSensitivityByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("caseSensitivity", Json.toJson(record));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " case_sensitive info cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatasetCaseSensitivity() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetCaseSensitivity(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset case_sensitive info updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetReference()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        List<DatasetReferenceRecord> records = DatasetInfoDao.getDatasetReferenceByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("references", Json.toJson(records));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " reference cannot be found!");
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
        List<DatasetReferenceRecord> records = DatasetInfoDao.getDatasetReferenceByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("references", Json.toJson(records));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " reference info cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatasetReference() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetReference(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset reference updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetPartition()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        DatasetPartitionRecord record = DatasetInfoDao.getDatasetPartitionByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("partitionSpec", Json.toJson(record));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " partition info cannot be found!");
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
        DatasetPartitionRecord record = DatasetInfoDao.getDatasetPartitionByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("partitionSpec", Json.toJson(record));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " partition info cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatasetPartition() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetPartition(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset partition info updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetCompliance()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        DatasetComplianceRecord record = DatasetInfoDao.getDatasetComplianceByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("privacyCompliancePolicy", Json.toJson(record));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " privacy compliance info cannot be found!");
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
        DatasetComplianceRecord record = DatasetInfoDao.getDatasetComplianceByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("privacyCompliancePolicy", Json.toJson(record));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " privacy compliance info cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatasetCompliance() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetCompliance(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset privacy compliance info updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetSecurity()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        DatasetSecurityRecord record = DatasetInfoDao.getDatasetSecurityByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("securitySpecification", Json.toJson(record));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " security info cannot be found!");
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
        DatasetSecurityRecord record = DatasetInfoDao.getDatasetSecurityByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("securitySpecification", Json.toJson(record));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " security info cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatasetSecurity() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetSecurity(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset security info updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetOwner()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        List<DatasetOwnerRecord> owners = DatasetInfoDao.getDatasetOwnerByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("owners", Json.toJson(owners));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException dataset id: " + datasetId, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + datasetId + " owner info cannot be found!");
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
        List<DatasetOwnerRecord> owners = DatasetInfoDao.getDatasetOwnerByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("owners", Json.toJson(owners));
      } catch (EmptyResultDataAccessException e) {
        Logger.debug("DataAccessException urn: " + urn, e);
        resultJson.put("return_code", 404);
        resultJson.put("error_message", "dataset " + urn + " owner info cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatasetOwner() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetOwner(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset owner info updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetConstraint()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();
    String datasetIdString = request().getQueryString("datasetId");
    if (datasetIdString != null) {
      int datasetId = Integer.parseInt(datasetIdString);

      try {
        List<DatasetConstraintRecord> constraints = DatasetInfoDao.getDatasetConstraintByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("constraints", Json.toJson(constraints));
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
        List<DatasetConstraintRecord> constraints = DatasetInfoDao.getDatasetConstraintByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("constraints", Json.toJson(constraints));
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
      DatasetInfoDao.updateDatasetConstraint(root);
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
        List<DatasetIndexRecord> indices = DatasetInfoDao.getDatasetIndexByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("indices", Json.toJson(indices));
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
        List<DatasetIndexRecord> indices = DatasetInfoDao.getDatasetIndexByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("indices", Json.toJson(indices));
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
      DatasetInfoDao.updateDatasetIndex(root);
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
        DatasetSchemaInfoRecord schema = DatasetInfoDao.getDatasetSchemaByDatasetId(datasetId);
        resultJson.put("return_code", 200);
        resultJson.set("schemas", Json.toJson(schema));
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
        DatasetSchemaInfoRecord schema = DatasetInfoDao.getDatasetSchemaByDatasetUrn(urn);
        resultJson.put("return_code", 200);
        resultJson.set("schemas", Json.toJson(schema));
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
      DatasetInfoDao.updateDatasetSchema(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset schema updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getDatasetInventoryItems()
      throws SQLException {
    ObjectNode resultJson = Json.newObject();

    String dataPlatform = request().getQueryString("dataPlatform");
    String nativeName = request().getQueryString("nativeName");
    String dataOrigin = request().getQueryString("dataOrigin");
    int limit = 1;
    try {
      limit = Integer.parseInt(request().getQueryString("limit"));
    } catch (NumberFormatException e) {
    }

    if (dataPlatform != null && nativeName != null && dataOrigin != null) {
      try {
        List<Map<String, Object>> items =
            DatasetInfoDao.getDatasetInventoryItems(dataPlatform, nativeName, dataOrigin, limit);
        resultJson.put("return_code", 200);
        resultJson.set("datasetList", Json.toJson(items));
      } catch (EmptyResultDataAccessException e) {
        resultJson.put("return_code", 404);
        resultJson.put("error_message",
            "dataset inventory for " + dataPlatform + " - " + nativeName + " - " + dataOrigin + " cannot be found!");
      }
      return ok(resultJson);
    }

    // if no parameter, return an error message
    resultJson.put("return_code", 400);
    resultJson.put("error_message", "No parameter provided");
    return ok(resultJson);
  }

  @BodyParser.Of(BodyParser.Json.class)
  public static Result updateDatesetInventory() {
    JsonNode root = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      DatasetInfoDao.updateDatasetInventory(root);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dataset inventory updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }
}
