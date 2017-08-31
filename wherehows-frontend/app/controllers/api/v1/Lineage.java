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
import dao.DatasetsDAO;
import dao.LineageDAO;
import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.Play;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import wherehows.models.table.Dataset;


public class Lineage extends Controller {
  private static final String LINEAGE_LOOK_BACK_TIME_KEY = "lineage.look.back.time";

  public static Result getDatasetLineageGraphData(int id) {
    ObjectNode result = Json.newObject();
    String username = session("user");
    if (id < 1) {
      result.put("status", "error");
      result.put("message", "wrong dataset id");
      return ok(result);
    }

    Dataset dataset = DatasetsDAO.getDatasetByID(id, username);
    if (dataset == null || StringUtils.isBlank(dataset.urn)) {
      result.put("status", "error");
      result.put("message", "wrong dataset id");
      return ok(result);
    }

    int upLevel = 1;
    String upLevelStr = request().getQueryString("upLevel");
    if (StringUtils.isBlank(upLevelStr)) {
      upLevel = 1;
    } else {
      try {
        upLevel = Integer.parseInt(upLevelStr);
      } catch (NumberFormatException e) {
        Logger.error(
            "Lineage Controller getDatasetLineageGraphData wrong upLevel parameter. Error message: " + e.getMessage());
        upLevel = 1;
      }
    }
    if (upLevel < 1) {
      upLevel = 1;
    }

    int downLevel = 1;
    String downLevelStr = request().getQueryString("downLevel");
    if (StringUtils.isBlank(downLevelStr)) {
      downLevel = 1;
    } else {
      try {
        downLevel = Integer.parseInt(downLevelStr);
      } catch (NumberFormatException e) {
        Logger.error("Lineage Controller getDatasetLineageGraphData wrong downLevel parameter. Error message: "
            + e.getMessage());
        downLevel = 1;
      }
    }
    if (downLevel < 1) {
      downLevel = 1;
    }

    int lookBackTimeDefault =
        Integer.valueOf(Play.application().configuration().getString(LINEAGE_LOOK_BACK_TIME_KEY, "30"));
    int lookBackTime = lookBackTimeDefault;
    String lookBackTimeStr = request().getQueryString("period");
    if (!StringUtils.isBlank(lookBackTimeStr)) {
      try {
        lookBackTime = Integer.parseInt(lookBackTimeStr);
      } catch (NumberFormatException e) {
        Logger.error(
            "Lineage Controller getDatasetLineageGraphData wrong period parameter. Error message: " + e.getMessage());
        lookBackTime = lookBackTimeDefault;
      }
    }

    result.put("status", "ok");
    result.set("data", Json.toJson(LineageDAO.getObjectAdjacnet(dataset.urn, upLevel, downLevel, lookBackTime)));
    return ok(result);
  }

  public static Result getFlowLineageGraphData(String application, String project, Long flowId) {
    ObjectNode result = Json.newObject();

    result.put("status", "ok");
    result.set("data", Json.toJson(LineageDAO.getFlowLineage(application, project, flowId)));
    return ok(result);
  }
}
