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

import com.fasterxml.jackson.databind.node.ObjectNode;
import dao.SearchDAO;
import java.util.List;
import org.apache.commons.lang3.math.NumberUtils;
import play.Logger;
import play.Play;
import play.cache.Cache;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import static org.apache.commons.lang3.StringUtils.*;


public class Search extends Controller {
  private static final String AUTOCOMPLETE_ALL_KEY = "autocomplete.all";
  private static final String AUTOCOMPLETE_DATASET_KEY = "autocomplete.dataset";
  private static final String AUTOCOMPLETE_FLOW_KEY = "autocomplete.flow";
  private static final String AUTOCOMPLETE_METRIC_KEY = "autocomplete.metric";
  private static final int DEFAULT_AUTOCOMPLETE_SIZE = 20;
  private static final int DEFAULT_AUTOCOMPLETE_CACHE_TIME = 3600; // cache for an hour

  public static Result getSearchAutoComplete() {
    // if not input, then get all search names (without limit).
    String input = request().getQueryString("input");
    int size = 0;  // size 0 means no limit
    if (isNotBlank(input)) {
      size = NumberUtils.toInt(request().getQueryString("size"), DEFAULT_AUTOCOMPLETE_SIZE);
    }

    String cacheKey = AUTOCOMPLETE_ALL_KEY + (isNotBlank(input) ? "." + input : "-all");
    List<String> names = (List<String>) Cache.get(cacheKey);
    if (names == null || names.size() == 0) {
      names = SearchDAO.getAutoCompleteList(input, size);
      Cache.set(cacheKey, names, DEFAULT_AUTOCOMPLETE_CACHE_TIME);
    }

    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.put("input", input);
    result.set("source", Json.toJson(names));
    return ok(result);
  }

  public static Result getSearchAutoCompleteForDataset() {
    // if not input, then get all search names (without limit).
    String input = request().getQueryString("input");
    int size = 0;  // size 0 means no limit
    if (isNotBlank(input)) {
      size = NumberUtils.toInt(request().getQueryString("size"), DEFAULT_AUTOCOMPLETE_SIZE);
    }

    String cacheKey = AUTOCOMPLETE_DATASET_KEY + (isNotBlank(input) ? "." + input : "-all");
    List<String> names = (List<String>) Cache.get(cacheKey);
    if (names == null || names.size() == 0) {
      names = SearchDAO.getAutoCompleteListDataset(input, size);
      Cache.set(cacheKey, names, DEFAULT_AUTOCOMPLETE_CACHE_TIME);
    }

    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.put("input", input);
    result.set("source", Json.toJson(names));
    return ok(result);
  }

  public static Result getSearchAutoCompleteForMetric() {
    // if not input, then get all search names (without limit).
    String input = request().getQueryString("input");
    int size = 0;  // size 0 means no limit
    if (isNotBlank(input)) {
      size = NumberUtils.toInt(request().getQueryString("size"), DEFAULT_AUTOCOMPLETE_SIZE);
    }

    String cacheKey = AUTOCOMPLETE_METRIC_KEY + (isNotBlank(input) ? "." + input : "-all");
    List<String> names = (List<String>) Cache.get(cacheKey);
    if (names == null || names.size() == 0) {
      names = SearchDAO.getAutoCompleteListMetric(input, size);
      Cache.set(cacheKey, names, DEFAULT_AUTOCOMPLETE_CACHE_TIME);
    }

    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.put("input", input);
    result.set("source", Json.toJson(names));
    return ok(result);
  }

  public static Result getSearchAutoCompleteForFlow() {
    // if not input, then get all search names (without limit).
    String input = request().getQueryString("input");
    int size = 0;  // size 0 means no limit
    if (isNotBlank(input)) {
      size = NumberUtils.toInt(request().getQueryString("size"), DEFAULT_AUTOCOMPLETE_SIZE);
    }

    String cacheKey = AUTOCOMPLETE_FLOW_KEY + (isNotBlank(input) ? "." + input : "-all");
    List<String> names = (List<String>) Cache.get(cacheKey);
    if (names == null || names.size() == 0) {
      names = SearchDAO.getAutoCompleteListFlow(input, size);
      Cache.set(cacheKey, names, DEFAULT_AUTOCOMPLETE_CACHE_TIME);
    }

    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.put("input", input);
    result.set("source", Json.toJson(names));
    return ok(result);
  }

  public static Result searchByKeyword() {
    ObjectNode result = Json.newObject();

    int page = 1;
    int size = 10;
    String keyword = request().getQueryString("keyword");
    String category = request().getQueryString("category");
    String source = request().getQueryString("source");
    String pageStr = request().getQueryString("page");
    if (isBlank(pageStr)) {
      page = 1;
    } else {
      try {
        page = Integer.parseInt(pageStr);
      } catch (NumberFormatException e) {
        Logger.error("Dataset Controller searchByKeyword wrong page parameter. Error message: " + e.getMessage());
        page = 1;
      }
    }

    String sizeStr = request().getQueryString("size");
    if (isBlank(sizeStr)) {
      size = 10;
    } else {
      try {
        size = Integer.parseInt(sizeStr);
      } catch (NumberFormatException e) {
        Logger.error("Dataset Controller searchByKeyword wrong page parameter. Error message: " + e.getMessage());
        size = 10;
      }
    }

    result.put("status", "ok");
    Boolean isDefault = false;
    if (isBlank(category)) {
      category = "datasets";
    }
    if (isBlank(source) || source.equalsIgnoreCase("all") || source.equalsIgnoreCase("default")) {
      source = null;
    }

    String searchEngine = Play.application().configuration().getString(SearchDAO.WHEREHOWS_SEARCH_ENGINE_KEY);

    if (category.toLowerCase().equalsIgnoreCase("metrics")) {
      if (isNotBlank(searchEngine) && searchEngine.equalsIgnoreCase("elasticsearch")) {
        result.set("result", SearchDAO.elasticSearchMetricByKeyword(category, keyword, page, size));
      } else {
        result.set("result", SearchDAO.getPagedMetricByKeyword(category, keyword, page, size));
      }
    } else if (category.toLowerCase().equalsIgnoreCase("flows")) {
      if (isNotBlank(searchEngine) && searchEngine.equalsIgnoreCase("elasticsearch")) {
        result.set("result", SearchDAO.elasticSearchFlowByKeyword(category, keyword, page, size));
      } else {
        result.set("result", SearchDAO.getPagedFlowByKeyword(category, keyword, page, size));
      }
    } else if (category.toLowerCase().equalsIgnoreCase("jobs")) {
      if (isNotBlank(searchEngine) && searchEngine.equalsIgnoreCase("elasticsearch")) {
        result.set("result", SearchDAO.elasticSearchFlowByKeyword(category, keyword, page, size));
      } else {
        result.set("result", SearchDAO.getPagedJobByKeyword(category, keyword, page, size));
      }
    } else if (category.toLowerCase().equalsIgnoreCase("comments")) {
      if (isNotBlank(searchEngine) && searchEngine.equalsIgnoreCase("elasticsearch")) {
        result.set("result", SearchDAO.elasticSearchDatasetByKeyword(category, keyword, null, page, size));
      } else {
        result.set("result", SearchDAO.getPagedCommentsByKeyword(category, keyword, page, size));
      }
    } else {
      if (isNotBlank(searchEngine) && searchEngine.equalsIgnoreCase("elasticsearch")) {
        result.set("result", SearchDAO.elasticSearchDatasetByKeyword(category, keyword, source, page, size));
      } else {
        result.set("result", SearchDAO.getPagedDatasetByKeyword(category, keyword, source, page, size));
      }
    }

    return ok(result);
  }
}
