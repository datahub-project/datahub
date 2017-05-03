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
package models.daos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;

import play.libs.Json;
import utils.JdbcUtil;

/**
 * Created by nvemuri on 6/29/16.
 * Modified by nvemuri on 6/29/16.
 */
public class UserDao {

  private final static String GET_USERS_WATCHED_DATASET =
      "SELECT u.username AS username FROM users u JOIN watch w ON " +
          "u.id = w.user_id JOIN dict_dataset d ON w.item_id = d.id " +
          "WHERE d.name = :name AND w.item_type = 'dataset' ";

  public static ObjectNode getWatchers(String datasetName) {
    List<String> watchUsers = new ArrayList<String>();
    if (StringUtils.isNotBlank(datasetName)) {
      Map<String, Object> params = new HashMap<>();
      params.put("name", datasetName);
      List<Map<String, Object>> rows = null;
      rows = JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_USERS_WATCHED_DATASET, params);
      for (Map row : rows) {
        String userName = (String) row.get("username");
        watchUsers.add(userName);
      }
    }
    ObjectNode result = Json.newObject();
    result.put("count", watchUsers.size());
    result.set("users", Json.toJson(watchUsers));
    return result;
  }
}
