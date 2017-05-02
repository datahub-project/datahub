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

import com.fasterxml.jackson.databind.JsonNode;
import utils.JdbcUtil;
import utils.JsonUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by zechen on 10/21/15.
 */
public class CfgDao {

  public static final String FIND_APP_INFO_BY_NAME = "SELECT * FROM cfg_application where short_connection_string = :name";
  public static final String FIND_APP_INFO_BY_APP_CODE = "SELECT * FROM cfg_application where app_code = :app_code";
  public static final String FIND_DB_INFO_BY_NAME = "SELECT * from cfg_database where short_connection_string = :name";
  public static final String FIND_APP_INFO_BY_ID = "SELECT * FROM cfg_application where app_id = :id";
  public static final String FIND_DB_INFO_BY_ID = "SELECT * from cfg_database where db_id = :id";
  public static final String GET_ALL_APPS = "SELECT * FROM cfg_application order by app_id";
  public static final String GET_ALL_DBS = "SELECT * from cfg_database order by db_id";
  public static final String INSERT_NEW_APP = "INSERT INTO cfg_application (app_id, app_code, description, uri, short_connection_string, tech_matrix_id, parent_app_id, app_status, is_logical) "
    + "VALUES (:appId, :appCode, :description, :uri, :shortConnectionString, :techMatrixId, :parentAppId, :appStatus, :isLogical)";
  public static final String UPDATE_APP = "UPDATE cfg_application SET app_code = :appCode, description = :description, uri = :uri, short_connection_string = :shortConnectionString, " +
    " tech_matrix_id = :techMatrixId, parent_app_id = :parentAppId, app_status = :appStatus, is_logical = :isLogical WHERE app_id = :appId";
  public static final String INSERT_NEW_DB = "INSERT INTO cfg_database (db_id, db_code, primary_dataset_type, description, cluster_size, associated_dc_num, replication_role, uri, short_connection_string, jdbc_url, is_logical) "
    + "VALUES (:dbId, :dbCode, :datasetType, :description, :clusterSize, :associatedDcNum, :replicationRole, :uri, :shortConnectionString, :jdbcUrl, :isLogical)";
  public static final String UPDATE_DB = "UPDATE cfg_database SET db_code = :dbCode, primary_dataset_type = :datasetType, description = :description, cluster_size = :clusterSize, associated_dc_num = :associatedDcNum, " +
    " replication_role = :replicationRole, uri = :uri, short_connection_string = :shortConnectionString, jdbc_url = :jdbcUrl, is_logical = :isLogical WHERE db_id = :dbId";


  public static Map<String, Object> getAppByName(String name) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("name", name);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(FIND_APP_INFO_BY_NAME, params);
  }

  public static Map<String, Object> getAppByAppCode(String appCode)
      throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("app_code", appCode);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(FIND_APP_INFO_BY_APP_CODE, params);
  }

  public static Map<String, Object> getDbByName(String name) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("name", name);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(FIND_DB_INFO_BY_NAME, params);
  }

  public static Map<String, Object> getAppById(int id) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("id", id);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(FIND_APP_INFO_BY_ID, params);
  }

  public static Map<String, Object> getDbById(int id) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("id", id);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(FIND_DB_INFO_BY_ID, params);
  }

  public static List<Map<String, Object>> getAllApps() throws Exception {
    return JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_ALL_APPS);
  }

  public static List<Map<String, Object>> getAllDbs() throws Exception {
    return JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_ALL_DBS);
  }

  public static void insertApp(JsonNode app) throws Exception {
    insertOrUpdateApp(app, true);
  }

  public static void updateApp(JsonNode app) throws Exception {
    insertOrUpdateApp(app, false);
  }

  public static void insertOrUpdateApp(JsonNode app, boolean isInsert) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("appId", JsonUtil.getJsonValue(app, "app_id", Integer.class));
    params.put("appCode", JsonUtil.getJsonValue(app, "app_code", String.class, null));
    params.put("description", JsonUtil.getJsonValue(app, "description", String.class, null));
    params.put("uri", JsonUtil.getJsonValue(app, "uri", String.class, null));
    params.put("shortConnectionString", JsonUtil.getJsonValue(app, "short_connection_string", String.class));
    params.put("techMatrixId", JsonUtil.getJsonValue(app, "techMatrixId", Integer.class, null));
    params.put("parentAppId", JsonUtil.getJsonValue(app, "parent_app_id", Integer.class, null));
    params.put("appStatus", JsonUtil.getJsonValue(app, "app_status", String.class, null));
    params.put("isLogical", ((boolean) JsonUtil.getJsonValue(app, "is_logical", Boolean.class, false) ? "Y" : "N"));

    if (isInsert) {
      JdbcUtil.wherehowsNamedJdbcTemplate.update(INSERT_NEW_APP, params);
    } else {
      JdbcUtil.wherehowsNamedJdbcTemplate.update(UPDATE_APP, params);
    }
  }


  public static void insertDb(JsonNode db) throws Exception {
    insertOrUpdateDb(db, true);
  }

  public static void updateDb(JsonNode db) throws Exception {
    insertOrUpdateDb(db, false);
  }

  public static void insertOrUpdateDb(JsonNode db, boolean isInsert) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("dbId", JsonUtil.getJsonValue(db, "db_id", Integer.class));
    params.put("dbCode", JsonUtil.getJsonValue(db, "db_code", String.class, null));
    params.put("description", JsonUtil.getJsonValue(db, "description", String.class, null));
    params.put("datasetType", JsonUtil.getJsonValue(db, "primary_dataset_type", String.class, null));
    params.put("clusterSize", JsonUtil.getJsonValue(db, "cluster_size", Integer.class, null));
    params.put("associatedDcNum", JsonUtil.getJsonValue(db, "associated_dc_num", Integer.class, null));
    params.put("replicationRole", JsonUtil.getJsonValue(db, "replication_role", String.class, null));
    params.put("uri", JsonUtil.getJsonValue(db, "uri", String.class, null));
    params.put("shortConnectionString", JsonUtil.getJsonValue(db, "short_connection_string", String.class));
    params.put("jdbcUrl", JsonUtil.getJsonValue(db, "jdbc_url", String.class, null));
    params.put("isLogical", ((boolean) JsonUtil.getJsonValue(db, "is_logical", Boolean.class, false) ? "Y" : "N"));

    if (isInsert) {
      JdbcUtil.wherehowsNamedJdbcTemplate.update(INSERT_NEW_DB, params);
    } else {
      JdbcUtil.wherehowsNamedJdbcTemplate.update(UPDATE_DB, params);
    }
  }
}
