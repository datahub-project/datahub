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
import metadata.etl.models.EtlJobName;
import org.springframework.jdbc.support.KeyHolder;
import play.Logger;
import utils.JdbcUtil;
import utils.JsonUtil;
import utils.PasswordManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Created by zechen on 9/24/15.
 */
public class EtlJobPropertyDao {
  public static final String INSERT_JOB_PROPERTY =
    "INSERT INTO wh_etl_job_property(wh_etl_job_name, ref_id, ref_id_type, property_name, property_value, is_encrypted)"
      + "VALUES(:etlJobName, :refId, :refIdType, :propertyName, :propertyValue, :isEncrypted)";

  public static final String INSERT_WHEREHOWS_PROPERTY =
    "INSERT INTO wh_property(property_name, property_value, is_encrypted, group_name)"
      + "VALUES(:propertyName, :propertyValue, :isEncrypted, :groupName)";

  public static final String UPDATE_JOB_PROPERTY =
      "INSERT INTO wh_etl_job_property(wh_etl_job_name, ref_id, ref_id_type, property_name, property_value, is_encrypted)"
          + "VALUES(:etlJobName, :refId, :refIdType, :propertyName, :propertyValue, :isEncrypted)"
        + "ON DUPLICATE KEY UPDATE property_value = :propertyValue, is_encrypted = :isEncrypted";

  public static final String GET_JOB_PROPERTIES =
    "SELECT * FROM wh_etl_job_property WHERE wh_etl_job_name = :etlJobName and ref_id = :refId";

  public static final String GET_JOB_PROPERTY =
    "SELECT * FROM wh_etl_job_property WHERE wh_etl_job_name = :etlJobName and ref_id = :refId and property_name = :propertyName";

  public static final String GET_WHEREHOWS_PROPERTIES = "SELECT * FROM wh_property";

  public static final String GET_WHEREHOWS_PROPERTY = "SELECT * FROM wh_property WHERE property_name = :propertyName";

  public static final String DEFAULT_MASTER_KEY_LOC = System.getProperty("user.home") + "/.wherehows/master_key";

  public static int insertJobProperty(EtlJobName etlJobName, Integer refId, String propertyName, String propertyValue,
    boolean isEncrypted)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("etlJobName", etlJobName.toString());
    params.put("refId", refId);
    params.put("refIdType", etlJobName.getRefIdType().toString());
    params.put("propertyName", propertyName);
    if (isEncrypted) {
      params.put("propertyValue", encrypt(propertyValue));
      params.put("isEncrypted", "Y");
    } else {
      params.put("propertyValue", propertyValue);
      params.put("isEncrypted", "N");
    }
    KeyHolder kh = JdbcUtil.insertRow(INSERT_JOB_PROPERTY, params);
    return kh.getKey().intValue();
  }

  public static int insertWherehowsProperty(String propertyName, String propertyValue, boolean isEncrypted,
    String groupName)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("propertyName", propertyName);
    params.put("propertyValue", propertyValue);
    if (isEncrypted) {
      params.put("propertyValue", encrypt(propertyValue));
      params.put("isEncrypted", "Y");
    } else {
      params.put("propertyValue", propertyValue);
      params.put("isEncrypted", "N");
    }
    params.put("groupName", groupName);
    KeyHolder kh = JdbcUtil.insertRow(INSERT_WHEREHOWS_PROPERTY, params);
    return kh.getKey().intValue();
  }

  public static void updateJobProperty(JsonNode jobProperty)
    throws Exception {

    EtlJobName whEtlJobName = EtlJobName.valueOf((String) JsonUtil.getJsonValue(jobProperty, "wh_etl_job_name", String.class));
    int refId = (Integer) JsonUtil.getJsonValue(jobProperty, "ref_id", Integer.class);
    String propertyName = (String) JsonUtil.getJsonValue(jobProperty, "property_name", String.class);
    String propertyValue = (String) JsonUtil.getJsonValue(jobProperty, "property_value", String.class);
    boolean needEncrypted = (boolean) JsonUtil.getJsonValue(jobProperty, "need_encrypted", Boolean.class, false);
    updateJobProperty(whEtlJobName, refId, propertyName, propertyValue, needEncrypted);
  }

  public static void updateJobProperty(EtlJobName etlJobName, Integer refId, String propertyName, String propertyValue,
    boolean isEncrypted)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("etlJobName", etlJobName.toString());
    params.put("refId", refId);
    params.put("refIdType", etlJobName.getRefIdType().toString());
    params.put("propertyName", propertyName);
    if (isEncrypted) {
      params.put("propertyValue", encrypt(propertyValue));
      params.put("isEncrypted", "Y");
    } else {
      params.put("propertyValue", propertyValue);
      params.put("isEncrypted", "N");
    }
    JdbcUtil.wherehowsNamedJdbcTemplate.update(UPDATE_JOB_PROPERTY, params);
  }

  public static Properties getJobProperties(EtlJobName etlJobName, Integer refId)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("etlJobName", etlJobName.toString());
    params.put("refId", refId);
    List<Map<String, Object>> rows = JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_JOB_PROPERTIES, params);
    Properties ret = new Properties();
    for (Map<String, Object> row : rows) {
      if ((row.get("is_encrypted")).equals("N")) {
        ret.put(row.get("property_name"), row.get("property_value"));
      } else {
        ret.put(row.get("property_name"), decrypt((String) row.get("property_value")));
      }
    }
    return ret;
  }

  public static String getJobProperty(EtlJobName etlJobName, Integer refId, String propertyName)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("etlJobName", etlJobName.toString());
    params.put("refId", refId);
    params.put("propertyName", propertyName);
    Map<String, Object> row = JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_JOB_PROPERTY, params);
    if ((row.get("is_encrypted")).equals("N")) {
      return (String) row.get("property_value");
    } else {
      return decrypt((String) row.get("property_value"));
    }
  }

  public static Properties getWherehowsProperties()
    throws Exception {
    List<Map<String, Object>> rows = JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_WHEREHOWS_PROPERTIES);
    Properties ret = new Properties();
    for (Map<String, Object> row : rows) {
      if ((row.get("is_encrypted")).equals("N")) {
        ret.put(row.get("property_name"), row.get("property_value"));
      } else {
        ret.put(row.get("property_name"), decrypt((String) row.get("property_value")));
      }
    }
    return ret;
  }

  public static String getWherehowsProperty(String propertyName)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("propertyName", propertyName);
    Map<String, Object> row = JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_WHEREHOWS_PROPERTY, params);
    if ((row.get("is_encrypted")).equals("N")) {
      return (String) row.get("property_value");
    } else {
      return decrypt((String) row.get("property_value"));
    }
  }

  public static String encrypt(String value)
    throws Exception {
    String masterKeyLoc = getMasterKeyLoc();
    return PasswordManager.encryptPassword(value, masterKeyLoc);
  }

  public static String decrypt(String value)
    throws Exception {
    String masterKeyLoc = getMasterKeyLoc();
    return PasswordManager.decryptPassword(value, masterKeyLoc);
  }

  private static String getMasterKeyLoc()
    throws Exception {
    try {
      return getWherehowsProperty("wherehows.encrypt.master.key.loc");
    } catch (Exception e) {
      e.printStackTrace();
      Logger.warn("master key location is not found, using default location: {}", DEFAULT_MASTER_KEY_LOC);
    }
    return DEFAULT_MASTER_KEY_LOC;
  }
}
