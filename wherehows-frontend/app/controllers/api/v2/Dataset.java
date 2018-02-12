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

import controllers.Application;
import java.util.Collections;
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
import wherehows.dao.view.DataTypesViewDao;
import wherehows.dao.view.DatasetViewDao;

import static utils.Dataset.*;


public class Dataset extends Controller {

  private static final DatasetViewDao DATASET_VIEW_DAO = Application.DAO_FACTORY.getDatasetViewDao();

  private static final DataTypesViewDao DATA_TYPES_DAO = Application.DAO_FACTORY.getDataTypesViewDao();

  private static final int _dataset_search_page_size = 20;

  private Dataset() {
  }

  public static Promise<Result> listSegments(@Nullable String platform, @Nonnull String prefix) {
    try {
      if (StringUtils.isBlank(platform)) {
        return Promise.promise(() -> ok(Json.toJson(
            DATA_TYPES_DAO.getAllPlatforms().stream().map(s -> s.get("name")).collect(Collectors.toList()))));
      }

      List<String> names = DATASET_VIEW_DAO.listSegments(platform, "PROD", getPlatformPrefix(platform, prefix));

      // if prefix is a dataset name, then return empty list
      if (names.size() == 1 && names.get(0).equalsIgnoreCase(prefix)) {
        return Promise.promise(() -> ok(Json.toJson(Collections.emptyList())));
      }

      return Promise.promise(() -> ok(Json.toJson(names)));
    } catch (Exception e) {
      Logger.error("Fail to list dataset names/sections", e);
      return Promise.promise(() -> internalServerError("Fetch data Error: " + e.toString()));
    }
  }

  public static Promise<Result> listDatasets(@Nullable String platform, @Nonnull String prefix) {
    try {
      int page = NumberUtils.toInt(request().getQueryString("page"), 0);
      int start = page * _dataset_search_page_size;

      return Promise.promise(() -> ok(
          Json.toJson(DATASET_VIEW_DAO.listDatasets(platform, "PROD", prefix, start, _dataset_search_page_size))));
    } catch (Exception e) {
      Logger.error("Fail to list datasets", e);
      return Promise.promise(() -> internalServerError("Fetch data Error: " + e.toString()));
    }
  }

  public static Promise<Result> countDatasets(@Nullable String platform, @Nonnull String prefix) {
    try {
      return Promise.promise(
          () -> ok(String.valueOf(DATASET_VIEW_DAO.listDatasets(platform, "PROD", prefix, 0, 1).getTotal())));
    } catch (Exception e) {
      Logger.error("Fail to count total datasets", e);
      return Promise.promise(() -> internalServerError("Fetch data Error: " + e.toString()));
    }
  }

  public static Promise<Result> getComplianceDataTypes() {
    try {
      return Promise.promise(() -> ok(
          Json.newObject().set("complianceDataTypes", Json.toJson(DATA_TYPES_DAO.getAllComplianceDataTypes()))));
    } catch (Exception e) {
      Logger.error("Fail to get compliance data types", e);
      return Promise.promise(() -> notFound("Fetch data Error: " + e.toString()));
    }
  }

  public static Promise<Result> getDataPlatforms() {
    try {
      return Promise.promise(
          () -> ok(Json.newObject().set("platforms", Json.toJson(DATA_TYPES_DAO.getAllPlatforms()))));
    } catch (Exception e) {
      Logger.error("Fail to get data platforms", e);
      return Promise.promise(() -> notFound("Fetch data Error: " + e.toString()));
    }
  }
}
