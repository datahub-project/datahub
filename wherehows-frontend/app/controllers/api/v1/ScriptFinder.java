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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dao.ScriptFinderDAO;
import java.net.URLDecoder;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;


public class ScriptFinder extends Controller {

  public static Result getAllScriptTypes() {
    ObjectNode result = Json.newObject();

    result.put("status", "ok");
    result.set("scriptTypes", Json.toJson(ScriptFinderDAO.getAllScriptTypes()));
    return ok(result);
  }

  public static Result getScripts() {
    ObjectNode result = Json.newObject();

    int page = 1;
    String pageStr = request().getQueryString("page");
    if (StringUtils.isBlank(pageStr)) {
      page = 1;
    } else {
      try {
        page = Integer.parseInt(pageStr);
      } catch (NumberFormatException e) {
        Logger.error("ScriptFinder Controller getPagedScripts wrong page parameter. Error message: " + e.getMessage());
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
        Logger.error("ScriptFinder Controller getPagedScripts wrong size parameter. Error message: " + e.getMessage());
        size = 10;
      }
    }

    String filterOptStr = request().getQueryString("query");
    JsonNode filterOpt = null;
    try {
      String filterOptStrDecode = URLDecoder.decode(filterOptStr, "UTF-8");
      filterOpt = Json.parse(filterOptStrDecode);
    } catch (Exception e) {
      Logger.error(
          "ScriptFinder Controller getScripts query filterOpt wrong JSON format. Error message: " + e.getMessage());
      filterOpt = null;
    }

    result.put("status", "ok");
    result.set("data", ScriptFinderDAO.getPagedScripts(filterOpt, page, size));
    return ok(result);
  }

  public static Result getScriptRuntimeByJobID(int applicationID, int jobID) {
    ObjectNode result = Json.newObject();

    int attemptNumber = 0;
    String attemptStr = request().getQueryString("attempt_number");
    if (StringUtils.isBlank(attemptStr)) {
      attemptNumber = 0;
    } else {
      try {
        attemptNumber = Integer.parseInt(attemptStr);
      } catch (NumberFormatException e) {
        Logger.error("ScriptFinder Controller getPagedScripts wrong page parameter. Error message: " + e.getMessage());
        attemptNumber = 0;
      }
    }

    result.put("status", "ok");
    result.set("data", Json.toJson(ScriptFinderDAO.getPagedScriptRuntime(applicationID, jobID)));
    return ok(result);
  }

  public static Result getScriptLineageByJobID(int applicationID, int jobID) {
    ObjectNode result = Json.newObject();

    int attemptNumber = 0;
    String attemptStr = request().getQueryString("attempt_number");
    if (StringUtils.isBlank(attemptStr)) {
      attemptNumber = 0;
    } else {
      try {
        attemptNumber = Integer.parseInt(attemptStr);
      } catch (NumberFormatException e) {
        Logger.error("ScriptFinder Controller getPagedScripts wrong page parameter. Error message: " + e.getMessage());
        attemptNumber = 0;
      }
    }

    result.put("status", "ok");
    result.set("data", Json.toJson(ScriptFinderDAO.getScriptLineage(applicationID, jobID)));
    return ok(result);
  }
}