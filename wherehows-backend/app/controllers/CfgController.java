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
import models.daos.CfgDao;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import java.util.List;
import java.util.Map;


/**
 * Created by zechen on 10/27/15.
 */
public class CfgController extends Controller {

  public static Result getAllApps() {
    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> apps = CfgDao.getAllApps();
      resultJson.put("return_code", 200);
      resultJson.set("applications", Json.toJson(apps));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result getAllDbs() {
    ObjectNode resultJson = Json.newObject();
    try {
      List<Map<String, Object>> dbs = CfgDao.getAllDbs();
      resultJson.put("return_code", 200);
      resultJson.set("databases", Json.toJson(dbs));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result getAppById(int id) {
    ObjectNode resultJson = Json.newObject();
    try {
      Map<String, Object> app = CfgDao.getAppById(id);
      resultJson.put("return_code", 200);
      resultJson.set("application", Json.toJson(app));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result getAppByName(String name) {
    ObjectNode resultJson = Json.newObject();
    try {
      Map<String, Object> app = CfgDao.getAppByName(name);
      resultJson.put("return_code", 200);
      resultJson.set("application", Json.toJson(app));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result getDbById(int id) {
    ObjectNode resultJson = Json.newObject();
    try {
      Map<String, Object> db = CfgDao.getDbById(id);
      resultJson.put("return_code", 200);
      resultJson.set("database", Json.toJson(db));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result getDbByName(String name) {
    ObjectNode resultJson = Json.newObject();
    try {
      Map<String, Object> db = CfgDao.getDbByName(name);
      resultJson.put("return_code", 200);
      resultJson.set("database", Json.toJson(db));
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result addApp() {
    JsonNode app = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      CfgDao.insertApp(app);
      resultJson.put("return_code", 200);
      resultJson.put("message", "New application created!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result addDb() {
    JsonNode db = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      CfgDao.insertDb(db);
      resultJson.put("return_code", 200);
      resultJson.put("message", "New database created!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result updateApp() {
    JsonNode app = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      CfgDao.updateApp(app);
      resultJson.put("return_code", 200);
      resultJson.put("message", "Application updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }

  public static Result updateDb() {
    JsonNode db = request().body().asJson();
    ObjectNode resultJson = Json.newObject();
    try {
      CfgDao.updateDb(db);
      resultJson.put("return_code", 200);
      resultJson.put("message", "App updated!");
    } catch (Exception e) {
      e.printStackTrace();
      resultJson.put("return_code", 404);
      resultJson.put("error_message", e.getMessage());
    }

    return ok(resultJson);
  }
}
