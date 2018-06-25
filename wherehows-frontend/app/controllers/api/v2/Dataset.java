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
package controllers.api.v2;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import controllers.Application;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import play.Logger;
import play.libs.F.Promise;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.mvc.Results;
import wherehows.dao.table.AclDao;
import wherehows.dao.table.DatasetComplianceDao;
import wherehows.dao.table.DatasetOwnerDao;
import wherehows.dao.table.DictDatasetDao;
import wherehows.dao.table.LineageDao;
import wherehows.dao.view.DataTypesViewDao;
import wherehows.dao.view.DatasetViewDao;
import wherehows.dao.view.OwnerViewDao;
import wherehows.models.table.AccessControlEntry;
import wherehows.models.view.DatasetCompliance;
import wherehows.models.view.DatasetOwner;
import wherehows.models.view.DatasetOwnership;
import wherehows.models.view.DatasetRetention;
import wherehows.models.view.DatasetSchema;
import wherehows.models.view.DatasetView;
import wherehows.models.view.DsComplianceSuggestion;

import static controllers.api.v1.Dataset.*;
import static utils.Dataset.*;


public class Dataset extends Controller {

  private static final DataTypesViewDao DATA_TYPES_DAO = Application.DAO_FACTORY.getDataTypesViewDao();

  private static final DictDatasetDao DICT_DATASET_DAO = Application.DAO_FACTORY.getDictDatasetDao();

  private static final DatasetViewDao DATASET_VIEW_DAO = Application.DAO_FACTORY.getDatasetViewDao();

  private static final OwnerViewDao OWNER_VIEW_DAO = Application.DAO_FACTORY.getOwnerViewDao();

  private static final DatasetOwnerDao OWNER_DAO = Application.DAO_FACTORY.getDatasteOwnerDao();

  private static final DatasetComplianceDao COMPLIANCE_DAO = Application.DAO_FACTORY.getDatasetComplianceDao();

  private static final LineageDao LINEAGE_DAO = Application.DAO_FACTORY.getLineageDao();

  private static final AclDao ACL_DAO = initAclDao();

  private static final int _DEFAULT_PAGE_SIZE = 20;

  private static final long _DEFAULT_JIT_ACL_PERIOD = 48 * 3600;  // 48 hour in seconds

  private static final JsonNode _EMPTY_RESPONSE = Json.newObject();

  private Dataset() {
  }

  private static AclDao initAclDao() {
    try {
      return Application.DAO_FACTORY.getAclDao();
    } catch (Exception e) {
      Logger.error("ACL DAO init error", e);
    }
    return null;
  }

  public static Promise<Result> listSegments(@Nullable String platform, @Nonnull String prefix) {
    try {
      if (StringUtils.isBlank(platform)) {
        return Promise.promise(() -> ok(Json.toJson(DATA_TYPES_DAO.getAllPlatforms()
            .stream()
            .map(s -> String.format("[platform=%s]", s.get("name")))
            .collect(Collectors.toList()))));
      }

      List<String> names = DATASET_VIEW_DAO.listSegments(platform, "PROD", getPlatformPrefix(platform, prefix));

      // if prefix is a dataset name, then return empty list
      if (names.size() == 1 && names.get(0).equalsIgnoreCase(prefix)) {
        return Promise.promise(() -> ok(Json.newArray()));
      }

      return Promise.promise(() -> ok(Json.toJson(names)));
    } catch (Exception e) {
      Logger.error("Fail to list dataset names/sections", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
  }

  public static Promise<Result> listDatasets(@Nullable String platform, @Nonnull String prefix) {
    try {
      int start = NumberUtils.toInt(request().getQueryString("start"), 0);
      int count = NumberUtils.toInt(request().getQueryString("count"), _DEFAULT_PAGE_SIZE);
      int page = NumberUtils.toInt(request().getQueryString("page"), 0);
      // 'start' takes precedence over 'page'
      int startIndex = (request().getQueryString("start") == null && page > 0) ? page * _DEFAULT_PAGE_SIZE : start;

      return Promise.promise(
          () -> ok(Json.toJson(DATASET_VIEW_DAO.listDatasets(platform, "PROD", prefix, startIndex, count))));
    } catch (Exception e) {
      Logger.error("Fail to list datasets", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
  }

  public static Promise<Result> countDatasets(@Nullable String platform, @Nonnull String prefix) {
    try {
      return Promise.promise(
          () -> ok(String.valueOf(DATASET_VIEW_DAO.listDatasets(platform, "PROD", prefix, 0, 1).getTotal())));
    } catch (Exception e) {
      Logger.error("Fail to count total datasets", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
  }

  public static Promise<Result> getComplianceDataTypes() {
    try {
      return Promise.promise(() -> ok(
          Json.newObject().set("complianceDataTypes", Json.toJson(DATA_TYPES_DAO.getAllComplianceDataTypes()))));
    } catch (Exception e) {
      Logger.error("Fail to get compliance data types", e);
      return Promise.promise(() -> notFound(errorResponse(e)));
    }
  }

  public static Promise<Result> getFieldFormatDocs() {
    try {
      return Promise.promise(() -> ok(
          Json.newObject().set("fieldFormatDocs", Json.toJson(DATA_TYPES_DAO.getDocsForAllFieldFormats()))));
    } catch (Exception e) {
      Logger.error("Fail to get docs of field formats", e);
      return Promise.promise(() -> notFound(errorResponse(e)));
    }
  }

  public static Promise<Result> getDataPlatforms() {
    try {
      return Promise.promise(
          () -> ok(Json.newObject().set("platforms", Json.toJson(DATA_TYPES_DAO.getAllPlatforms()))));
    } catch (Exception e) {
      Logger.error("Fail to get data platforms", e);
      return Promise.promise(() -> notFound(errorResponse(e)));
    }
  }

  public static Promise<Result> getWhUrnById(int id) {
    String whUrn = getDatasetUrnByIdOrCache(id);

    if (whUrn != null) {
      response().setHeader("whUrn", whUrn);
      return Promise.promise(Results::ok);
    } else {
      return Promise.promise(Results::notFound);
    }
  }

  public static Promise<Result> getDataset(@Nonnull String datasetUrn) {
    final DatasetView view;
    try {
      view = DATASET_VIEW_DAO.getDatasetView(datasetUrn);
    } catch (Exception e) {
      if (e.toString().contains("Response status 404")) {
        return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
      }

      Logger.error("Failed to get dataset view", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }

    return Promise.promise(() -> ok(Json.newObject().set("dataset", Json.toJson(view))));
  }

  public static Promise<Result> updateDatasetDeprecation(String datasetUrn) {
    final String username = session("user");
    if (StringUtils.isBlank(username)) {
      return Promise.promise(() -> unauthorized(_EMPTY_RESPONSE));
    }

    try {
      JsonNode record = request().body().asJson();

      boolean deprecated = record.get("deprecated").asBoolean();
      String deprecationNote = record.hasNonNull("deprecationNote") ? record.get("deprecationNote").asText() : "";
      long decommissionTime = record.hasNonNull("decommissionTime") ? record.get("decommissionTime").asLong() : 0;
      if (deprecated && decommissionTime <= 0) {
        throw new IllegalArgumentException("Invalid decommission time");
      }

      DICT_DATASET_DAO.setDatasetDeprecation(datasetUrn, deprecated, deprecationNote, decommissionTime, username);
    } catch (Exception e) {
      Logger.error("Update dataset deprecation fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }

    return Promise.promise(() -> ok(_EMPTY_RESPONSE));
  }

  public static Promise<Result> getDatasetSchema(String datasetUrn) {
    final DatasetSchema schema;
    try {
      schema = DATASET_VIEW_DAO.getDatasetSchema(datasetUrn);
    } catch (Exception e) {
      if (e.toString().contains("Response status 404")) {
        return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
      }

      Logger.error("Fetch schema fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }

    if (schema == null) {
      return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
    }
    return Promise.promise(() -> ok(Json.newObject().set("schema", Json.toJson(schema))));
  }

  public static Promise<Result> getDatasetOwners(String datasetUrn) {
    final DatasetOwnership ownership;
    try {
      ownership = OWNER_VIEW_DAO.getDatasetOwners(datasetUrn);
    } catch (Exception e) {
      if (e.toString().contains("Response status 404")) {
        return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
      }

      Logger.error("Fetch owners fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(Json.toJson(ownership)));
  }

  public static Promise<Result> getDatasetSuggestedOwners(String datasetUrn) {
    final DatasetOwnership ownership;
    try {
      ownership = OWNER_VIEW_DAO.getDatasetSuggestedOwners(datasetUrn);
    } catch (Exception e) {
      if (e.toString().contains("Response status 404")) {
        return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
      }

      Logger.error("Fetch owners fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(Json.toJson(ownership)));
  }

  public static Promise<Result> updateDatasetOwners(String datasetUrn) {
    final String username = session("user");
    if (StringUtils.isBlank(username)) {
      return Promise.promise(() -> unauthorized(_EMPTY_RESPONSE));
    }

    final JsonNode content = request().body().asJson();
    // content should contain arraynode 'owners': []
    if (content == null || !content.has("owners") || !content.get("owners").isArray()) {
      return Promise.promise(() -> badRequest(errorResponse("Update dataset owners fail: missing owners field")));
    }

    try {
      final List<DatasetOwner> owners = Json.mapper().readerFor(new TypeReference<List<DatasetOwner>>() {
      }).readValue(content.get("owners"));

      long confirmedOwnerUserCount = owners.stream()
          .filter(s -> "DataOwner".equalsIgnoreCase(s.getType()) && "user".equalsIgnoreCase(s.getIdType())
              && "UI".equalsIgnoreCase(s.getSource()))
          .count();

      // enforce at least two UI (confirmed) USER DataOwner for a dataset before making any changes
      if (confirmedOwnerUserCount < 2) {
        return Promise.promise(() -> badRequest(errorResponse("Less than 2 UI USER owners")));
      }

      OWNER_DAO.updateDatasetOwners(datasetUrn, owners, username);
    } catch (Exception e) {
      Logger.error("Update Dataset owners fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(_EMPTY_RESPONSE));
  }

  public static Promise<Result> getDatasetCompliance(@Nonnull String datasetUrn) {
    final DatasetCompliance record;
    try {
      record = COMPLIANCE_DAO.getDatasetComplianceByUrn(datasetUrn);
    } catch (Exception e) {
      if (e.toString().contains("Response status 404")) {
        return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
      }

      Logger.error("Fetch compliance fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }

    return Promise.promise(() -> ok(Json.newObject().set("complianceInfo", Json.toJson(record))));
  }

  public static Promise<Result> updateDatasetCompliance(@Nonnull String datasetUrn) {
    final String username = session("user");
    if (StringUtils.isBlank(username)) {
      return Promise.promise(() -> unauthorized(_EMPTY_RESPONSE));
    }

    try {
      DatasetCompliance record = Json.mapper().convertValue(request().body().asJson(), DatasetCompliance.class);

      if (record.getDatasetUrn() == null || !record.getDatasetUrn().equals(datasetUrn)) {
        throw new IllegalArgumentException("Dataset Urn not exist or doesn't match.");
      }

      COMPLIANCE_DAO.updateDatasetComplianceByUrn(record, username);
    } catch (Exception e) {
      Logger.error("Update Compliance Info fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(_EMPTY_RESPONSE));
  }

  public static Promise<Result> getDatasetSuggestedCompliance(@Nonnull String datasetUrn) {
    final DsComplianceSuggestion record;
    try {
      record = COMPLIANCE_DAO.getComplianceSuggestion(datasetUrn);
    } catch (Exception e) {
      if (e.toString().contains("Response status 404")) {
        return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
      }

      Logger.error("Fetch compliance suggestion fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }

    if (record == null) {
      return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
    }
    return Promise.promise(() -> ok(Json.newObject().set("complianceSuggestion", Json.toJson(record))));
  }

  public static Promise<Result> sendDatasetSuggestedComplianceFeedback(@Nonnull String datasetUrn) {
    try {
      JsonNode record = request().body().asJson();
      String feedback = record.hasNonNull("feedback") ? record.get("feedback").asText().toUpperCase() : null;
      String uid = record.hasNonNull("uid") ? record.get("uid").asText() : "";
      if (!"ACCEPT".equals(feedback) && !"REJECT".equals(feedback)) {
        return Promise.promise(() -> badRequest(_EMPTY_RESPONSE));
      }

      COMPLIANCE_DAO.sendSuggestedComplianceFeedback(datasetUrn, uid, feedback);
    } catch (Exception e) {
      Logger.error("Send compliance suggestion feedback fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(_EMPTY_RESPONSE));
  }

  public static Promise<Result> getDatasetRetention(@Nonnull String datasetUrn) {
    final DatasetRetention record;
    try {
      record = COMPLIANCE_DAO.getDatasetRetention(datasetUrn);
    } catch (Exception e) {
      if (e.toString().contains("Response status 404")) {
        return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
      }

      Logger.error("Fetch compliance fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }

    if (record == null) {
      return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
    }
    return Promise.promise(() -> ok(Json.newObject().set("retentionPolicy", Json.toJson(record))));
  }

  public static Promise<Result> updateDatasetRetention(@Nonnull String datasetUrn) {
    final String username = session("user");
    if (StringUtils.isBlank(username)) {
      return Promise.promise(() -> unauthorized(_EMPTY_RESPONSE));
    }

    try {
      DatasetRetention record = Json.mapper().convertValue(request().body().asJson(), DatasetRetention.class);

      if (record.getDatasetUrn() == null || !record.getDatasetUrn().equals(datasetUrn)) {
        throw new IllegalArgumentException("Dataset Urn not exist or doesn't match.");
      }

      COMPLIANCE_DAO.updateDatasetRetention(record, username);
    } catch (Exception e) {
      Logger.error("Update Retention Policy fail", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(_EMPTY_RESPONSE));
  }

  public static Promise<Result> getDatasetAcls(@Nonnull String datasetUrn) {
    final List<AccessControlEntry> acls;
    try {
      acls = ACL_DAO.getDatasetAcls(datasetUrn);
    } catch (Exception e) {
      if (e.toString().contains("Response status 404")) {
        return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
      }

      Logger.error("Fetch ACLs error", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }

    if (acls == null) {
      return Promise.promise(() -> notFound(_EMPTY_RESPONSE));
    }
    return Promise.promise(() -> ok(Json.toJson(acls)));
  }

  public static Promise<Result> addUserToDatasetAcl(@Nonnull String datasetUrn) {
    final String username = session("user");
    if (StringUtils.isBlank(username)) {
      return Promise.promise(() -> unauthorized(_EMPTY_RESPONSE));
    }

    JsonNode record = request().body().asJson();

    String accessType = record.hasNonNull("accessType") ? record.get("accessType").asText() : "r"; // default read
    Long expiresAt = record.hasNonNull("expiresAt") ? record.get("expiresAt").asLong()
        : System.currentTimeMillis() / 1000 + _DEFAULT_JIT_ACL_PERIOD; // default now + 48h, in seconds
    if (!record.hasNonNull("businessJustification")) {
      return Promise.promise(() -> badRequest(errorResponse("Missing business justification")));
    }
    String businessJustification = record.get("businessJustification").asText();

    try {
      ACL_DAO.addUserToDatasetAcl(datasetUrn, username, accessType, businessJustification, expiresAt);
    } catch (Exception e) {
      Logger.error("Add user to ACL error", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(_EMPTY_RESPONSE));
  }

  public static Promise<Result> removeUserFromDatasetAcl(@Nonnull String datasetUrn) {
    final String username = session("user");
    if (StringUtils.isBlank(username)) {
      return Promise.promise(() -> unauthorized(_EMPTY_RESPONSE));
    }

    try {
      ACL_DAO.removeUserFromDatasetAcl(datasetUrn, username);
    } catch (Exception e) {
      Logger.error("Remove User from ACL error", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(_EMPTY_RESPONSE));
  }

  public static Promise<Result> getDatasetUpstreams(@Nonnull String datasetUrn) {
    final List<DatasetView> upstreams;
    try {
      upstreams = LINEAGE_DAO.getUpstreamDatasets(datasetUrn);
    } catch (Exception e) {
      Logger.error("Fetch Dataset upstreams error", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(Json.toJson(upstreams)));
  }

  public static Promise<Result> getDatasetDownstreams(@Nonnull String datasetUrn) {
    final List<DatasetView> downstreams;
    try {
      downstreams = LINEAGE_DAO.getDownstreamDatasets(datasetUrn);
    } catch (Exception e) {
      Logger.error("Fetch Dataset upstreams error", e);
      return Promise.promise(() -> internalServerError(errorResponse(e)));
    }
    return Promise.promise(() -> ok(Json.toJson(downstreams)));
  }

  private static <E extends Throwable> JsonNode errorResponse(E e) {
    return errorResponse(e.toString());
  }

  private static JsonNode errorResponse(String msg) {
    return Json.newObject().put("msg", msg);
  }
}
