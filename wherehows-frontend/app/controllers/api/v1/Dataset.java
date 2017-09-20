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
package controllers.api.v1;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import controllers.Application;
import dao.AbstractMySQLOpenSourceDAO;
import dao.DatasetsDAO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import play.Logger;
import play.cache.Cache;
import play.libs.F.Promise;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import wherehows.dao.table.DatasetClassificationDao;
import wherehows.dao.table.DatasetsDao;
import wherehows.dao.view.DatasetViewDao;
import wherehows.dao.view.OwnerViewDao;
import wherehows.models.table.DatasetClassification;
import wherehows.models.view.DatasetColumn;
import wherehows.models.table.DatasetCompliance;
import wherehows.models.table.DatasetDependency;
import wherehows.models.view.DatasetOwner;
import wherehows.models.table.ImpactDataset;


public class Dataset extends Controller {
  private static final JdbcTemplate JDBC_TEMPLATE = AbstractMySQLOpenSourceDAO.getJdbcTemplate();

  private static final NamedParameterJdbcTemplate NAMED_JDBC_TEMPLATE =
      AbstractMySQLOpenSourceDAO.getNamedParameterJdbcTemplate();

  private static final DatasetsDao DATASETS_DAO = Application.DAO_FACTORY.getDatasetsDao();

  private static final DatasetClassificationDao CLASSIFICATION_DAO =
      Application.DAO_FACTORY.getDatasetClassificationDao();

  private static final DatasetViewDao DATASET_VIEW_DAO = Application.DAO_FACTORY.getDatasetViewDao();

  private static final OwnerViewDao OWNER_VIEW_DAO = Application.DAO_FACTORY.getOwnerViewDao();

  private static final String URN_CACHE_KEY = "wh.urn.cache.";
  private static final int URN_CACHE_PERIOD = 24 * 3600; // cache for 24 hours

  public static Result getDatasetOwnerTypes() {
    ObjectNode result = Json.newObject();

    result.put("status", "ok");
    result.set("ownerTypes", Json.toJson(DatasetsDAO.getDatasetOwnerTypes()));
    return ok(result);
  }

  public static Result getPagedDatasets() {
    ObjectNode result = Json.newObject();
    String urn = request().getQueryString("urn");

    int page = 1;
    String pageStr = request().getQueryString("page");
    if (StringUtils.isBlank(pageStr)) {
      page = 1;
    } else {
      try {
        page = Integer.parseInt(pageStr);
      } catch (NumberFormatException e) {
        Logger.error("Dataset Controller getPagedDatasets wrong page parameter. Error message: " + e.getMessage());
        page = 1;
      }
    }

    int size = 10;
    String sizeStr = request().getQueryString("size");
    if (StringUtils.isBlank(sizeStr)) {
      size = 10;
    } else {
      try {
        size = Integer.parseInt(sizeStr);
      } catch (NumberFormatException e) {
        Logger.error("Dataset Controller getPagedDatasets wrong size parameter. Error message: " + e.getMessage());
        size = 10;
      }
    }

    result.put("status", "ok");
    String username = session("user");
    result.set("data", DatasetsDAO.getPagedDatasets(urn, page, size, username));
    return ok(result);
  }

  public static Result getDatasetByID(int id) {
    String username = session("user");
    wherehows.models.table.Dataset dataset = DatasetsDAO.getDatasetByID(id, username);

    ObjectNode result = Json.newObject();

    if (dataset != null) {
      result.put("status", "ok");
      result.set("dataset", Json.toJson(dataset));
    } else {
      result.put("status", "error");
      result.put("message", "record not found");
    }

    return ok(result);
  }

  public static Result getDatasetIdByUrn(String urn) {
    String cacheKey = URN_CACHE_KEY + urn;

    Integer datasetId = (Integer) Cache.get(cacheKey);
    if (datasetId != null && datasetId > 0) {
      response().setHeader("DatasetId", datasetId.toString());
      return ok();
    }

    datasetId = DATASETS_DAO.getDatasetIdByUrn(JDBC_TEMPLATE, urn);
    if (datasetId > 0) {
      Cache.set(cacheKey, datasetId, URN_CACHE_PERIOD);
      response().setHeader("DatasetId", datasetId.toString());
      return ok();
    } else {
      return notFound();
    }
  }

  public static Result getDatasetColumnByID(int datasetId, int columnId) {
    List<DatasetColumn> columns = DATASET_VIEW_DAO.getDatasetColumnByID(datasetId, columnId);

    ObjectNode result = Json.newObject();

    if (columns != null && columns.size() > 0) {
      result.put("status", "ok");
      result.set("columns", Json.toJson(columns));
    } else {
      result.put("status", "error");
      result.put("message", "record not found");
    }
    return ok(result);
  }

  public static Result getDatasetColumnsByID(int id) {
    String urn = DATASETS_DAO.validateUrn(JDBC_TEMPLATE, id);

    List<DatasetColumn> columns = DATASET_VIEW_DAO.getDatasetColumnsByID(id, urn);

    ObjectNode result = Json.newObject();

    if (columns != null && columns.size() > 0) {
      result.put("status", "ok");
      result.set("columns", Json.toJson(columns));
    } else {
      result.put("status", "error");
      result.put("message", "record not found");
    }
    return ok(result);
  }

  public static Result getDatasetPropertiesByID(int id) {
    JsonNode properties = DatasetsDAO.getDatasetPropertiesByID(id);

    ObjectNode result = Json.newObject();

    if (properties != null) {
      result.put("status", "ok");
      result.set("properties", properties);
    } else {
      result.put("status", "error");
      result.put("message", "record not found");
    }

    return ok(result);
  }

  public static Result getDatasetSampleDataByID(int id) {
    JsonNode sampleData = DatasetsDAO.getDatasetSampleDataByID(id);

    ObjectNode result = Json.newObject();

    if (sampleData != null) {
      result.put("status", "ok");
      result.set("sampleData", sampleData);
    } else {
      result.put("status", "error");
      result.put("message", "record not found");
    }

    return ok(result);
  }

  public static Result getDatasetImpactAnalysisByID(int id) {
    List<ImpactDataset> impactDatasetList = DatasetsDAO.getImpactAnalysisByID(id);

    ObjectNode result = Json.newObject();

    if (impactDatasetList != null) {
      result.put("status", "ok");
      result.set("impacts", Json.toJson(impactDatasetList));
    } else {
      result.put("status", "error");
      result.put("message", "record not found");
    }

    return ok(result);
  }

  public static Result getDatasetOwnersByID(int id) {
    ObjectNode result = Json.newObject();

    try {
      String urn = DATASETS_DAO.validateUrn(JDBC_TEMPLATE, id);

      result.set("owners", Json.toJson(OWNER_VIEW_DAO.getDatasetOwnersByUrn(urn)));
      result.put("status", "ok");
    } catch (Exception e) {
      Logger.warn("Failed to get owners: " + e.toString());
      result.put("status", "failed");
      result.put("message", "Error: " + e.getMessage());
    }

    return ok(result);
  }

  public static Result updateDatasetOwners(int id) {
    ObjectNode result = Json.newObject();
    String username = session("user");

    if (StringUtils.isBlank(username)) {
      result.put("status", "failed");
      result.put("error", "true");
      result.put("msg", "Unauthorized User.");
      return ok(result);
    }

    String urn = DATASETS_DAO.validateUrn(JDBC_TEMPLATE, id);

    Map<String, String[]> params = request().body().asFormUrlEncoded();
    // params should contain mapping 'owners': ['ownerInfoJsonString']
    if (params == null || !params.containsKey("owners") || params.get("owners") == null
        || params.get("owners").length == 0) {
      result.put("status", "failed");
      result.put("error", "true");
      result.put("msg", "Could not update dataset owners: missing fields");
      return ok(result);
    }
    final JsonNode node = Json.parse(params.get("owners")[0]);

    final List<DatasetOwner> owners = new ArrayList<>();
    int confirmedOwnerUserCount = 0;

    for (int i = 0; i < node.size(); i++) {
      final JsonNode ownerNode = node.get(i);
      if (ownerNode != null) {
        String userName = ownerNode.has("userName") ? ownerNode.get("userName").asText() : "";
        if (StringUtils.isBlank(userName)) {
          continue;
        }

        Boolean isGroup = ownerNode.has("isGroup") && ownerNode.get("isGroup").asBoolean();
        String type = ownerNode.has("type") && !ownerNode.get("type").isNull() ? ownerNode.get("type").asText() : "";
        String subType =
            ownerNode.has("subType") && !ownerNode.get("subType").isNull() ? ownerNode.get("subType").asText() : "";
        String confirmedBy =
            ownerNode.has("confirmedBy") && !ownerNode.get("confirmedBy").isNull() ? ownerNode.get("confirmedBy")
                .asText() : "";
        String idType = ownerNode.has("idType") ? ownerNode.get("idType").asText() : "";
        String source = ownerNode.has("source") ? ownerNode.get("source").asText() : "";

        DatasetOwner owner = new DatasetOwner();
        owner.setUserName(userName);
        owner.setIsGroup(isGroup);
        owner.setNamespace(isGroup ? "urn:li:corpGroup" : "urn:li:corpuser");
        owner.setType(type);
        owner.setSubType(subType);
        owner.setIdType(idType);
        owner.setSource(source);
        owner.setConfirmedBy(confirmedBy);
        owner.setSortId(i);
        owners.add(owner);

        if (type.equalsIgnoreCase("owner") && idType.equalsIgnoreCase("user") && StringUtils.isNotBlank(confirmedBy)) {
          confirmedOwnerUserCount++;
        }
      }
    }

    // enforce at least two confirmed owner for a dataset before making any changes
    if (confirmedOwnerUserCount < 2) {
      result.put("status", "failed");
      result.put("error", "true");
      result.put("msg", "Less than 2 confirmed owners.");
      return ok(result);
    }

    try {
      DATASETS_DAO.updateDatasetOwners(JDBC_TEMPLATE, username, id, urn, owners);
      result.put("status", "success");
    } catch (Exception e) {
      Logger.error("Owner updating error: ", e);
      result.put("status", "failed");
      result.put("error", "true");
      result.put("msg", "Could not update dataset owners: " + e.getMessage());
    }
    return ok(result);
  }

  public static Result favoriteDataset(int id) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    if (StringUtils.isNotBlank(username)) {
      if (DatasetsDAO.favorite(id, username)) {
        result.put("status", "success");
      } else {
        result.put("status", "failed");
      }
    } else {
      result.put("status", "failed");
    }

    return ok(result);
  }

  public static Result unfavoriteDataset(int id) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    if (StringUtils.isNotBlank(username)) {
      if (DatasetsDAO.unfavorite(id, username)) {
        result.put("status", "success");
      } else {
        result.put("status", "failed");
      }
    } else {
      result.put("status", "failed");
    }

    return ok(result);
  }

  public static Result getDatasetOwnedBy(String userId) {
    ObjectNode result = Json.newObject();
    int page = NumberUtils.toInt(request().getQueryString("page"), 1);
    int size = NumberUtils.toInt(request().getQueryString("size"), 10);

    if (StringUtils.isNotBlank(userId)) {
      result = DatasetsDAO.getDatasetOwnedBy(userId, page, size);
    } else {
      result.put("status", "failed, no user");
    }
    return ok(result);
  }

  public static Result ownDataset(int id) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    if (StringUtils.isNotBlank(username)) {
      result = DatasetsDAO.ownDataset(id, username);
    } else {
      result.put("status", "failed");
    }

    return ok(result);
  }

  public static Result unownDataset(int id) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    if (StringUtils.isNotBlank(username)) {
      result = DatasetsDAO.unownDataset(id, username);
    } else {
      result.put("status", "failed");
    }

    return ok(result);
  }

  public static Result getFavorites() {
    ObjectNode result = Json.newObject();
    String username = session("user");
    result.put("status", "ok");
    result.set("data", DatasetsDAO.getFavorites(username));
    return ok(result);
  }

  public static Result getPagedDatasetComments(int id) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    if (username == null) {
      username = "";
    }

    int page = 1;
    String pageStr = request().getQueryString("page");
    if (StringUtils.isBlank(pageStr)) {
      page = 1;
    } else {
      try {
        page = Integer.parseInt(pageStr);
      } catch (NumberFormatException e) {
        Logger.error(
            "Dataset Controller getPagedDatasetComments wrong page parameter. Error message: " + e.getMessage());
        page = 1;
      }
    }

    int size = 10;
    String sizeStr = request().getQueryString("size");
    if (StringUtils.isBlank(sizeStr)) {
      size = 10;
    } else {
      try {
        size = Integer.parseInt(sizeStr);
      } catch (NumberFormatException e) {
        Logger.error(
            "Dataset Controller getPagedDatasetComments wrong size parameter. Error message: " + e.getMessage());
        size = 10;
      }
    }

    result.put("status", "ok");
    result.set("data", DatasetsDAO.getPagedDatasetComments(username, id, page, size));
    return ok(result);
  }

  public static Result postDatasetComment(int id) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    Map<String, String[]> params = request().body().asFormUrlEncoded();

    if (StringUtils.isNotBlank(username)) {
      if (DatasetsDAO.postComment(id, params, username)) {
        result.put("status", "success");
        return ok(result);
      } else {
        result.put("status", "failed");
        result.put("error", "true");
        result.put("msg", "Could not create comment.");
        return badRequest(result);
      }
    } else {
      result.put("status", "failed");
      result.put("error", "true");
      result.put("msg", "Unauthorized User.");
      return badRequest(result);
    }
  }

  public static Result putDatasetComment(int id, int commentId) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    Map<String, String[]> params = request().body().asFormUrlEncoded();

    if (StringUtils.isNotBlank(username)) {
      if (DatasetsDAO.postComment(id, commentId, params, username)) {
        result.put("status", "success");
        return ok(result);
      } else {
        result.put("status", "failed");
        result.put("error", "true");
        result.put("msg", "Could not create comment.");
        return badRequest(result);
      }
    } else {
      result.put("status", "failed");
      result.put("error", "true");
      result.put("msg", "Unauthorized User");
      return badRequest(result);
    }
  }

  public static Result deleteDatasetComment(int id, int commentId) {
    ObjectNode result = Json.newObject();
    if (DatasetsDAO.deleteComment(commentId)) {
      result.put("status", "success");
    } else {
      result.put("status", "failed");
    }

    return ok(result);
  }

  public static Result watchDataset(int id) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    Map<String, String[]> params = request().body().asFormUrlEncoded();
    if (StringUtils.isNotBlank(username)) {
      String message = DatasetsDAO.watchDataset(id, params, username);
      if (StringUtils.isBlank(message)) {
        result.put("status", "success");
      } else {
        result.put("status", "failed");
        result.put("message", message);
      }
    } else {
      result.put("status", "failed");
      result.put("message", "User is not authenticated");
    }

    return ok(result);
  }

  public static Result getWatchedUrnId() {
    ObjectNode result = Json.newObject();
    String urn = request().getQueryString("urn");
    result.put("status", "success");
    Long id = 0L;

    if (StringUtils.isNotBlank(urn)) {
      String username = session("user");
      if (StringUtils.isNotBlank(username)) {
        id = DatasetsDAO.getWatchId(urn, username);
      }
    }
    result.put("id", id);

    return ok(result);
  }

  public static Result watchURN() {
    Map<String, String[]> params = request().body().asFormUrlEncoded();
    ObjectNode result = Json.newObject();

    String username = session("user");
    if (StringUtils.isNotBlank(username)) {
      String message = DatasetsDAO.watchURN(params, username);
      if (StringUtils.isBlank(message)) {
        result.put("status", "success");
      } else {
        result.put("status", "failed");
        result.put("message", message);
      }
    } else {
      result.put("status", "failed");
      result.put("message", "User is not authenticated");
    }
    return ok(result);
  }

  public static Result unwatchDataset(int id, int watchId) {
    ObjectNode result = Json.newObject();
    if (DatasetsDAO.unwatch(watchId)) {
      result.put("status", "success");
    } else {
      result.put("status", "failed");
    }

    return ok(result);
  }

  public static Result unwatchURN(int watchId) {
    ObjectNode result = Json.newObject();
    if (DatasetsDAO.unwatch(watchId)) {
      result.put("status", "success");
    } else {
      result.put("status", "failed");
    }

    return ok(result);
  }

  public static Result getPagedDatasetColumnComments(int datasetId, int columnId) {
    ObjectNode result = Json.newObject();

    String username = session("user");
    if (username == null) {
      username = "";
    }

    int page = 1;
    String pageStr = request().getQueryString("page");
    if (StringUtils.isBlank(pageStr)) {
      page = 1;
    } else {
      try {
        page = Integer.parseInt(pageStr);
      } catch (NumberFormatException e) {
        Logger.error(
            "Dataset Controller getPagedDatasetColumnComments wrong page parameter. Error message: " + e.getMessage());
        page = 1;
      }
    }

    int size = 10;
    String sizeStr = request().getQueryString("size");
    if (StringUtils.isBlank(sizeStr)) {
      size = 10;
    } else {
      try {
        size = Integer.parseInt(sizeStr);
      } catch (NumberFormatException e) {
        Logger.error(
            "Dataset Controller getPagedDatasetColumnComments wrong size parameter. Error message: " + e.getMessage());
        size = 10;
      }
    }

    result.put("status", "ok");
    result.set("data", DatasetsDAO.getPagedDatasetColumnComments(username, datasetId, columnId, page, size));
    return ok(result);
  }

  public static Result postDatasetColumnComment(int id, int columnId) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    Map<String, String[]> params = request().body().asFormUrlEncoded();
    if (StringUtils.isNotBlank(username)) {
      String errorMsg = DatasetsDAO.postColumnComment(id, columnId, params, username);
      if (StringUtils.isBlank(errorMsg)) {
        result.put("status", "success");
      } else {
        result.put("status", "failed");
        result.put("msg", errorMsg);
      }
    } else {
      result.put("status", "failed");
      result.put("msg", "Authentication Required");
    }

    return ok(result);
  }

  public static Result putDatasetColumnComment(int id, int columnId, int commentId) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    Map<String, String[]> params = request().body().asFormUrlEncoded();
    if (StringUtils.isNotBlank(username)) {
      String errorMsg = DatasetsDAO.postColumnComment(id, commentId, params, username);
      if (StringUtils.isBlank(errorMsg)) {
        result.put("status", "success");
      } else {
        result.put("status", "failed");
        result.put("msg", errorMsg);
      }
    } else {
      result.put("status", "failed");
      result.put("msg", "Authentication Required");
    }

    return ok(result);
  }

  public static Result assignCommentToColumn(int datasetId, int columnId) {
    ObjectNode json = Json.newObject();
    ArrayNode res = json.arrayNode();
    JsonNode req = request().body().asJson();
    if (req == null) {
      return badRequest("Expecting JSON data");
    }
    if (req.isArray()) {
      for (int i = 0; i < req.size(); i++) {
        JsonNode obj = req.get(i);
        Boolean isSuccess = DatasetsDAO.assignColumnComment(obj.get("datasetId").asInt(), obj.get("columnId").asInt(),
            obj.get("commentId").asInt());
        ObjectNode itemResponse = Json.newObject();
        if (isSuccess) {
          itemResponse.put("success", "true");
        } else {
          itemResponse.put("error", "true");
          itemResponse.put("datasetId", datasetId);
          itemResponse.put("columnId", columnId);
          itemResponse.set("commentId", obj.get("comment_id"));
        }
        res.add(itemResponse);
      }
    } else {
      Boolean isSuccess = DatasetsDAO.assignColumnComment(datasetId, columnId, req.get("commentId").asInt());
      ObjectNode itemResponse = Json.newObject();
      if (isSuccess) {
        itemResponse.put("success", "true");
      } else {
        itemResponse.put("error", "true");
        itemResponse.put("datasetId", datasetId);
        itemResponse.put("columnId", columnId);
        itemResponse.set("commentId", req.get("commentId"));
      }
      res.add(itemResponse);
    }
    ObjectNode result = Json.newObject();
    result.putArray("results").addAll(res);
    return ok(result);
  }

  public static Result deleteDatasetColumnComment(int id, int columnId, int commentId) {
    ObjectNode result = Json.newObject();
    if (DatasetsDAO.deleteColumnComment(id, columnId, commentId)) {
      result.put("status", "success");
    } else {
      result.put("status", "failed");
    }

    return ok(result);
  }

  public static Result getSimilarColumnComments(Long datasetId, int columnId) {
    ObjectNode result = Json.newObject();
    result.set("similar", Json.toJson(DatasetsDAO.similarColumnComments(datasetId, columnId)));
    return ok(result);
  }

  public static Result getSimilarColumns(int datasetId, int columnId) {
    ObjectNode result = Json.newObject();
    result.set("similar", Json.toJson(DatasetsDAO.similarColumns(datasetId, columnId)));
    return ok(result);
  }

  public static Result getDependViews(Long datasetId) {
    ObjectNode result = Json.newObject();
    List<DatasetDependency> depends = new ArrayList<>();
    DatasetsDAO.getDependencies(datasetId, depends);
    result.put("status", "ok");
    result.set("depends", Json.toJson(depends));
    return ok(result);
  }

  public static Result getReferenceViews(Long datasetId) {
    ObjectNode result = Json.newObject();
    List<DatasetDependency> references = new ArrayList<>();
    DatasetsDAO.getReferences(datasetId, references);
    result.put("status", "ok");
    result.set("references", Json.toJson(references));
    return ok(result);
  }

  public static Result getDatasetListNodes() {
    ObjectNode result = Json.newObject();
    String urn = request().getQueryString("urn");
    result.put("status", "ok");
    result.set("nodes", Json.toJson(DatasetsDAO.getDatasetListViewNodes(urn)));
    return ok(result);
  }

  public static Result getDatasetVersions(Long datasetId, Integer dbId) {
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("versions", Json.toJson(DatasetsDAO.getDatasetVersions(datasetId, dbId)));
    return ok(result);
  }

  public static Result getDatasetSchemaTextByVersion(Long datasetId, String version) {
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("schema_text", Json.toJson(DatasetsDAO.getDatasetSchemaTextByVersion(datasetId, version)));
    return ok(result);
  }

  public static Result getDatasetInstances(Long datasetId) {
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("instances", Json.toJson(DatasetsDAO.getDatasetInstances(datasetId)));
    return ok(result);
  }

  public static Result getDatasetPartitions(Long datasetId) {
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("partitions", Json.toJson(DatasetsDAO.getDatasetPartitionGains(datasetId)));
    return ok(result);
  }

  public static Result getDatasetAccess(Long datasetId) {
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("access", Json.toJson(DatasetsDAO.getDatasetAccessibilty(datasetId)));
    return ok(result);
  }

  public static Promise<Result> getDatasetCompliance(int datasetId) {
    DatasetCompliance record = null;
    try {
      record = DATASETS_DAO.getDatasetComplianceInfoByDatasetId(JDBC_TEMPLATE, datasetId);
    } catch (Exception e) {
      Logger.warn("Failed to get compliance: " + e.toString());
      JsonNode result = Json.newObject()
          .put("status", "failed")
          .put("error", "true")
          .put("msg", "Fetch data Error: " + e.getMessage());

      return Promise.promise(() -> ok(result));
    }

    JsonNode result = Json.newObject().put("status", "ok").set("complianceInfo", Json.toJson(record));

    return Promise.promise(() -> ok(result));
  }

  public static Promise<Result> updateDatasetCompliance(int datasetId) {
    String username = session("user");
    if (StringUtils.isBlank(username)) {
      JsonNode result = Json.newObject().put("status", "failed").put("error", "true").put("msg", "Unauthorized User.");

      return Promise.promise(() -> ok(result));
    }

    try {
      DatasetCompliance record = Json.mapper().convertValue(request().body().asJson(), DatasetCompliance.class);

      if (record.getDatasetId() != null && datasetId != record.getDatasetId()) {
        throw new IllegalArgumentException("Dataset id doesn't match.");
      }
      record.setDatasetId(datasetId);

      if (record.getDatasetUrn() == null) {
        record.setDatasetUrn(DATASETS_DAO.validateUrn(JDBC_TEMPLATE, datasetId));
      }

      DATASETS_DAO.updateDatasetComplianceInfo(NAMED_JDBC_TEMPLATE, record, username);
    } catch (Exception e) {
      JsonNode result = Json.newObject()
          .put("status", "failed")
          .put("error", "true")
          .put("msg", "Update Compliance Info Error: " + e.getMessage());

      Logger.warn("Update Compliance Info fail", e);

      return Promise.promise(() -> ok(result));
    }

    return Promise.promise(() -> ok(Json.newObject().put("status", "ok")));
  }

  public static Promise<Result> getDatasetAutoClassification(int datasetId) {
    DatasetClassification record = null;
    try {
      String urn = DATASETS_DAO.getDatasetUrnById(JDBC_TEMPLATE, datasetId);
      if (urn == null) {
        throw new IllegalArgumentException("Dataset not found, ID: " + datasetId);
      }

      record = trimDatasetClassification(CLASSIFICATION_DAO.getDatasetClassification(urn));
    } catch (Exception e) {
      JsonNode result = Json.newObject()
          .put("status", "failed")
          .put("error", "true")
          .put("msg", "Fetch data Error: " + e.getMessage());

      return Promise.promise(() -> ok(result));
    }

    JsonNode result = Json.newObject().put("status", "ok").set("autoClassification", Json.toJson(record));

    return Promise.promise(() -> ok(result));
  }

  /**
   * Trim not required information from DatasetClassification for UI, return a new record.
   * @param record DatasetClassification
   * @return DatasetClassification
   * @throws IOException
   */
  private static DatasetClassification trimDatasetClassification(DatasetClassification record) throws IOException {
    ObjectMapper mapper = Json.mapper();
    DatasetClassification newRecord = new DatasetClassification(record.getUrn(), null, record.getLastModified());

    List<Map<String, Object>> entities =
        mapper.readValue(record.getClassificationResult(), new TypeReference<List<Map<String, Object>>>() {
        });

    for (Map<String, Object> entity : entities) {
      entity.remove("dataType");
      Map<String, Object> identifierTypePrediction = (Map<String, Object>) entity.get("identifierTypePrediction");
      if (identifierTypePrediction != null) {
        identifierTypePrediction.remove("priority");
        identifierTypePrediction.remove("type");
        identifierTypePrediction.remove("exclusive");
      }
      Map<String, Object> logicalTypePrediction = (Map<String, Object>) entity.get("logicalTypePrediction");
      if (logicalTypePrediction != null) {
        logicalTypePrediction.remove("priority");
        logicalTypePrediction.remove("type");
        logicalTypePrediction.remove("exclusive");
      }
    }

    newRecord.setClassificationResult(mapper.writeValueAsString(entities));
    return newRecord;
  }
}
