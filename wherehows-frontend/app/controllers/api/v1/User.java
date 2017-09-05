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
import dao.UserDAO;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import play.cache.Cache;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import wherehows.models.table.CompanyUser;
import wherehows.models.table.Group;
import wherehows.models.table.UserEntity;


public class User extends Controller {
  private static final String CACHE_INTERNAL_USERS = "internal.users.cache";
  private static final String CACHE_INTERNAL_GROUPS = "internal.groups.cache";
  private static final String CACHE_INTERNAL_ENTITIES = "internal.entities.cache";

  public static Result getLoggedInUser() {
    ObjectNode result = Json.newObject();

    String username = session("user");
    if (StringUtils.isBlank(username)) {
      result.put("status", "failed");
      result.put("message", "no user in session");
      return ok(result);
    }

    wherehows.models.table.User user = UserDAO.getCurrentUser(username);
    if (StringUtils.isBlank(user.getUserName())) {
      result.put("status", "failed");
      result.put("message", "can't find user info");
      return ok(result);
    }

    result.set("user", Json.toJson(user));
    result.put("status", "ok");
    return ok(result);
  }

  public static Result updateSettings() {
    Map<String, String[]> params = request().body().asFormUrlEncoded();
    ObjectNode result = Json.newObject();

    String username = session("user");
    if (StringUtils.isNotBlank(username)) {
      String message = UserDAO.updateUserSettings(params, username);
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

  public static Result getAllCompanyUsers() {
    List<CompanyUser> users = (List<CompanyUser>) Cache.get(CACHE_INTERNAL_USERS);
    if (users == null || users.size() == 0) {
      users = UserDAO.getAllCompanyUsers();
      Cache.set(CACHE_INTERNAL_USERS, users, 24 * 3600); // cache for 24 hours
    }
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("employees", Json.toJson(users));
    return ok(result);
  }

  public static Result getAllGroups() {
    List<Group> groups = (List<Group>) Cache.get(CACHE_INTERNAL_GROUPS);
    if (groups == null || groups.size() == 0) {
      groups = UserDAO.getAllGroups();
      Cache.set(CACHE_INTERNAL_GROUPS, groups, 24 * 3600); // cache for 24 hours
    }
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("groups", Json.toJson(groups));
    return ok(result);
  }

  public static Result getAllUserEntities() {
    List<UserEntity> entities = (List<UserEntity>) Cache.get(CACHE_INTERNAL_ENTITIES);
    if (entities == null || entities.size() == 0) {
      entities = UserDAO.getAllUserEntities();
      Cache.set(CACHE_INTERNAL_ENTITIES, entities, 24 * 3600); // cache for 24 hours
    }
    ObjectNode result = Json.newObject();
    result.put("status", "ok");
    result.set("userEntities", Json.toJson(entities));
    return ok(result);
  }
}
