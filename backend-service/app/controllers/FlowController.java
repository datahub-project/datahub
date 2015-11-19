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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import models.daos.FlowDao;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;


/**
 * Created by zechen on 10/15/15.
 */
public class FlowController extends Controller {


  public static Result getFlowOwners(String flowPath) {
    String instance = request().getQueryString("instance");
    ObjectNode resultJson = Json.newObject();

    try {
      List<Map<String, Object>> owners = FlowDao.getFlowOwner(flowPath, instance);
      resultJson.put("return_code", 200);
      resultJson.set("owners", Json.toJson(owners));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }

  public static Result getFlowSchedules(String flowPath) {
    String instance = request().getQueryString("instance");
    ObjectNode resultJson = Json.newObject();

    try {
      List<Map<String, Object>> schedules = FlowDao.getFlowSchedules(flowPath, instance);
      resultJson.put("return_code", 200);
      resultJson.set("schedules", Json.toJson(schedules));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }
    return ok(resultJson);
  }
}
