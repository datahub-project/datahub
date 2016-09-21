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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import play.libs.Json;
import utils.JdbcUtil;
import wherehows.common.schemas.DatasetDependencyRecord;
import wherehows.common.schemas.DatasetRecord;
import wherehows.common.utils.PartitionPatternMatcher;
import wherehows.common.writers.DatabaseWriter;


/**
 * Created by zsun on 3/18/15.
 * Modified by zechen on 10/12/15.
 */
public class DatasetDao {

  public static final String GET_DATASET_BY_ID = "SELECT * FROM dict_dataset WHERE id = :id";
  public static final String GET_DATASET_BY_URN = "SELECT * FROM dict_dataset WHERE urn = :urn";

  public static final String DEFAULT_CLUSTER_NAME = "ltx1-holdem";
  public static final String CLUSTER_NAME_KEY = "cluster_name";
  public static final String DATASET_URI_KEY = "dataset_uri";
  public static final String HIVE_PREFIX_WITH_3_SLASH = "hive:///";
  public static final String DALIDS_PREFIX_WITH_3_SLASH = "dalids:///";
  public static final String HIVE_PREFIX_WITH_2_SLASH = "hive://";
  public static final String DALIDS_PREFIX_WITH_2_SLASH = "dalids://";

  public static final String GET_DATASET_ID_IN_MAP_TABLE_WITH_TYPE_AND_CLUSTER = "SELECT " +
          "c.object_dataset_id as dataset_id, d.urn, d.dataset_type, " +
          "i.deployment_tier, i.data_center, i.server_cluster " +
          "FROM cfg_object_name_map c JOIN dict_dataset d ON c.object_dataset_id = d.id " +
          "LEFT JOIN dict_dataset_instance i ON c.object_dataset_id = i.dataset_id " +
          "WHERE c.object_dataset_id is not null and  lower(c.object_name) = ? " +
          "and lower(c.object_type) = ? and lower(i.server_cluster) = ?";

  public static final String GET_DATASET_ID_IN_MAP_TABLE_WITH_CLUSTER = "SELECT c.object_dataset_id as dataset_id, " +
          "d.urn, d.dataset_type, i.deployment_tier, i.data_center, i.server_cluster " +
          "FROM cfg_object_name_map c JOIN dict_dataset d ON c.object_dataset_id = d.id " +
          "LEFT JOIN dict_dataset_instance i ON c.object_dataset_id = i.dataset_id " +
          "WHERE c.object_dataset_id is not null and  lower(c.object_name) = ? and lower(i.server_cluster) = ?";

  private final static String GET_DATASET_DEPENDS_VIEW = "SELECT object_type, object_sub_type, " +
          "object_name, map_phrase, is_identical_map, mapped_object_dataset_id, " +
          "mapped_object_type,  mapped_object_sub_type, mapped_object_name " +
          "FROM cfg_object_name_map WHERE object_dataset_id = ?";

  public final static String GET_DATASET_URN_PROPERTIES_LIKE_EXPR = "select urn from dict_dataset where properties like :properties";

  public static final String GET_DATASET_DEPENDENTS_IN_OBJ_MAP_TABLE_BY_ID = "SELECT " +
          "c.object_dataset_id as dataset_id, d.urn, d.dataset_type, c.object_sub_type " +
          "FROM cfg_object_name_map c JOIN dict_dataset d ON c.object_dataset_id = d.id " +
          "WHERE c.mapped_object_dataset_id = ?";

  public static final String GET_DATASET_DEPENDENTS_IN_OBJ_MAP_TABLE_BY_NAME = "SELECT " +
          "c.object_dataset_id as dataset_id, d.urn, d.dataset_type, c.object_sub_type " +
          "FROM cfg_object_name_map c JOIN dict_dataset d ON c.object_dataset_id = d.id " +
          "WHERE c.mapped_object_type = ? and " +
          "(c.mapped_object_name = ? or c.mapped_object_name like ?)";

  public static Map<String, Object> getDatasetById(int datasetId)
    throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put("id", datasetId);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_BY_ID, params);
  }

  public static Map<String, Object> getDatasetByUrn(String urn)
    throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put("urn", urn);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_BY_URN, params);
  }

  public static void insertDataset(JsonNode dataset)
    throws Exception {

    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    DatasetRecord record = om.convertValue(dataset, DatasetRecord.class);
    if (record.getRefDatasetUrn() != null) {
      Map<String, Object> refDataset = getDatasetByUrn(record.getRefDatasetUrn());
      // Find ref dataset id
      if (refDataset != null) {
        record.setRefDatasetId(((Long) refDataset.get("id")).intValue());
      }
    }


    // Find layout id
    if (record.getSamplePartitionFullPath() != null) {
      PartitionPatternMatcher ppm = new PartitionPatternMatcher(PartitionLayoutDao.getPartitionLayouts());
      record.setPartitionLayoutPatternId(ppm.analyze(record.getSamplePartitionFullPath()));
    }

    DatabaseWriter dw = new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, "dict_dataset");
    dw.append(record);
    dw.close();
  }

  public static void setDatasetRecord (JsonNode dataset)
    throws Exception {
    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    DatasetRecord record = om.convertValue(dataset, DatasetRecord.class);

    if (record != null) {
      Map<String, Object> params = new HashMap<>();
      params.put("urn", record.getUrn());
      try {
        Map<String, Object> result = JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_DATASET_BY_URN, params);
        updateDataset(dataset);
      } catch (EmptyResultDataAccessException e) {
        insertDataset(dataset);
      }
    }
  }

  public static void updateDataset(JsonNode dataset)
    throws Exception {
    ObjectMapper om = new ObjectMapper();
    om.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    DatasetRecord record = om.convertValue(dataset, DatasetRecord.class);
    if (record.getRefDatasetUrn() != null) {
      Map<String, Object> refDataset = getDatasetByUrn(record.getRefDatasetUrn());
      // Find ref dataset id
      if (refDataset != null) {
        record.setRefDatasetId(((Long) refDataset.get("id")).intValue());
      }
    }
    // Find layout id
    if (record.getSamplePartitionFullPath() != null) {
      PartitionPatternMatcher ppm = new PartitionPatternMatcher(PartitionLayoutDao.getPartitionLayouts());
      record.setPartitionLayoutPatternId(ppm.analyze(record.getSamplePartitionFullPath()));
    }

    DatabaseWriter dw = new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, "dict_dataset");
    dw.update(record.toUpdateDatabaseValue(), record.getUrn());
    dw.close();
  }

  public static int getDatasetDependencies(
          Long datasetId,
          String topologySortId,
          int level,
          List<DatasetDependencyRecord> depends)
  {
    if (depends == null)
    {
      depends = new ArrayList<DatasetDependencyRecord>();
    }

    List<Map<String, Object>> rows = null;
    rows = JdbcUtil.wherehowsJdbcTemplate.queryForList(
            GET_DATASET_DEPENDS_VIEW,
            datasetId);

    int index = 1;
    if (rows != null)
    {
      for (Map row : rows) {
        DatasetDependencyRecord datasetDependencyRecord = new DatasetDependencyRecord();
        datasetDependencyRecord.dataset_id = (Long) row.get("mapped_object_dataset_id");
        String objectName = (String) row.get("mapped_object_name");
        if (StringUtils.isNotBlank(objectName))
        {
          String[] info = objectName.split("/");
          if (info != null && info.length == 3)
          {
            datasetDependencyRecord.database_name = info[1];
            datasetDependencyRecord.table_name = info[2];
          }
        }
        datasetDependencyRecord.level_from_root = level;
        datasetDependencyRecord.type = (String) row.get("mapped_object_sub_type");
        datasetDependencyRecord.ref_obj_location = objectName;
        datasetDependencyRecord.ref_obj_type = (String) row.get("mapped_object_type");
        datasetDependencyRecord.topology_sort_id = topologySortId + Integer.toString((index++)*100);
        datasetDependencyRecord.next_level_dependency_count =
                getDatasetDependencies(datasetDependencyRecord.dataset_id,
                        datasetDependencyRecord.topology_sort_id,
                        level + 1,
                        depends);
        depends.add(datasetDependencyRecord);
      }
      return rows.size();
    }
    else
    {
      return 0;
    }
  }
  public static ObjectNode getDatasetDependency(JsonNode input)
          throws Exception {

    ObjectNode resultJson = Json.newObject();
    String cluster = DEFAULT_CLUSTER_NAME;
    String datasetUri = null;
    String dbName = null;
    String tableName = null;
    boolean isHive = false;
    boolean isDalids = false;
    if (input != null && input.isContainerNode())
    {
      if (input.has(CLUSTER_NAME_KEY))
      {
        cluster = input.get(CLUSTER_NAME_KEY).asText();
      }
      if (input.has(DATASET_URI_KEY))
      {
        datasetUri = input.get(DATASET_URI_KEY).asText();
      }
    }

    if (StringUtils.isBlank(datasetUri))
    {
      resultJson.put("return_code", 404);
      resultJson.put("message", "Wrong input format! Missing dataset uri");
      return resultJson;
    }

    Integer index = -1;
    if ((index = datasetUri.indexOf(HIVE_PREFIX_WITH_3_SLASH)) != -1)
    {
      isHive = true;
      String tmp = datasetUri.substring(index + HIVE_PREFIX_WITH_3_SLASH.length());
      String[] info = tmp.split("\\.|/");
      if (info != null && info.length == 2)
      {
        dbName = info[0];
        tableName = info[1];
      }
    }
    else if ((index = datasetUri.indexOf(DALIDS_PREFIX_WITH_3_SLASH)) != -1)
    {
      isDalids = true;
      String tmp = datasetUri.substring(index + DALIDS_PREFIX_WITH_3_SLASH.length());
      String[] info = tmp.split("\\.|/");
      if (info != null && info.length == 2)
      {
        dbName = info[0];
        tableName = info[1];
      }
    }
    else if ((index = datasetUri.indexOf(HIVE_PREFIX_WITH_2_SLASH)) != -1)
    {
      isHive = true;
      String tmp = datasetUri.substring(index + HIVE_PREFIX_WITH_2_SLASH.length());
      String[] info = tmp.split("\\.|/");
      if (info != null && info.length == 3)
      {
        cluster = info[0];
        dbName = info[1];
        tableName = info[2];
      }
    }
    else if ((index = datasetUri.indexOf(DALIDS_PREFIX_WITH_2_SLASH)) != -1)
    {
      isDalids = true;
      String tmp = datasetUri.substring(index + DALIDS_PREFIX_WITH_2_SLASH.length());
      String[] info = tmp.split("\\.|/");
      if (info != null && info.length == 3)
      {
        cluster = info[0];
        dbName = info[1];
        tableName = info[2];
      }
    }
    else if (datasetUri.indexOf('.') != -1)
    {
      index = datasetUri.indexOf(':');
      String tmp = datasetUri;
      if (index != -1)
      {
        cluster = datasetUri.substring(0, index);
        tmp = datasetUri.substring(index + 1);
      }
      String[] info = tmp.split("\\.|/");
      if (info != null && info.length == 2)
      {
        dbName = info[0];
        tableName = info[1];
      }
    }

    if (StringUtils.isBlank(cluster) || StringUtils.isBlank(dbName) || StringUtils.isBlank(tableName))
    {
      resultJson.put("return_code", 404);
      resultJson.put("message", "Wrong input format! Missing dataset uri");
      return resultJson;
    }

    String sqlQuery = null;
    List<Map<String, Object>> rows = null;

    if (isHive)
    {
      rows = JdbcUtil.wherehowsJdbcTemplate.queryForList(
                GET_DATASET_ID_IN_MAP_TABLE_WITH_TYPE_AND_CLUSTER,
                "/" + dbName + "/" + tableName,
                "hive",
                cluster);


    }
    else if (isDalids)
    {

      rows = JdbcUtil.wherehowsJdbcTemplate.queryForList(
                GET_DATASET_ID_IN_MAP_TABLE_WITH_TYPE_AND_CLUSTER,
                "/" + dbName + "/" + tableName,
                "dalids",
                cluster);
    }
    else
    {
      rows = JdbcUtil.wherehowsJdbcTemplate.queryForList(
                GET_DATASET_ID_IN_MAP_TABLE_WITH_CLUSTER,
                "/" + dbName + "/" + tableName, cluster);

    }

    Long datasetId = 0L;
    String urn = null;
    String datasetType = null;
    String deploymentTier = null;
    String dataCenter = null;
    String serverCluster = null;
    if (rows != null && rows.size() > 0) {
      for (Map row : rows) {

        datasetId = (Long) row.get("dataset_id");
        urn = (String) row.get("urn");
        datasetType = (String) row.get("dataset_type");
        if (datasetType.equalsIgnoreCase("hive"))
        {
          isHive = true;
        }
        else if (datasetType.equalsIgnoreCase("dalids"))
        {
          isDalids = true;
        }
        deploymentTier = (String) row.get("deployment_tier");
        dataCenter = (String) row.get("data_center");
        serverCluster = (String) row.get("server_cluster");
        break;
      }
    }
    else {
      resultJson.put("return_code", 200);
      resultJson.put("message", "Dependency information is not available.");
      return resultJson;
    }

    List<DatasetDependencyRecord> depends = new ArrayList<DatasetDependencyRecord>();
    getDatasetDependencies(datasetId, "", 1, depends);
    int leafLevelDependencyCount = 0;
    if (depends.size() > 0)
    {
      for(DatasetDependencyRecord d : depends)
      {
        if (d.next_level_dependency_count == 0)
        {
          leafLevelDependencyCount++;
        }
      }
    }
    StringBuilder inputUri = new StringBuilder("");
    if (isHive)
    {
      inputUri.append("hive://");
    }
    else if (isDalids)
    {
      inputUri.append("dalids://");
    }
    inputUri.append(cluster + "/" + dbName + "/" + tableName);

    resultJson.put("return_code", 200);
    resultJson.put("deployment_tier", deploymentTier);
    resultJson.put("data_center", dataCenter);
    resultJson.put("cluster", StringUtils.isNotBlank(serverCluster) ? serverCluster: cluster);
    resultJson.put("dataset_type", datasetType);
    resultJson.put("database_name", dbName);
    resultJson.put("table_name", tableName);
    resultJson.put("urn", urn);
    resultJson.put("dataset_id", datasetId);
    resultJson.put("input_uri", inputUri.toString());
    resultJson.set("dependencies", Json.toJson(depends));
    resultJson.put("leaf_level_dependency_count", leafLevelDependencyCount);
    return resultJson;
  }

   //
   public static ObjectNode getDatasetUrnForPropertiesLike(String properties) {
     ObjectNode result = Json.newObject();
     List<String> datasetUrns = new ArrayList<String>();
     if (StringUtils.isNotBlank(properties)) {
       Map<String, Object> params = new HashMap<>();
       params.put("properties", properties);
       List<Map<String, Object>> rows = null;
       rows = JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DATASET_URN_PROPERTIES_LIKE_EXPR, params);
       for (Map row : rows) {
         String datasetUrn = (String) row.get("urn");
         datasetUrns.add(datasetUrn);
       }
       result.put("count", datasetUrns.size());
       result.set("urns", Json.toJson(datasetUrns));
     }
     return result;
   }

   //
   public static List<Map<String, Object>> getDatasetDependents(Long datasetId) {
     if (datasetId > 0) {
       return JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_DATASET_DEPENDENTS_IN_OBJ_MAP_TABLE_BY_ID, datasetId);
     }
     return null;
   }

   //
   public static List<Map<String, Object>> getDatasetDependents(String dataPlatform, String path) {
     if (path.length() > 0 && dataPlatform.length() > 0) {
       String child_path = path + "/%";
       return JdbcUtil.wherehowsJdbcTemplate.queryForList(
               GET_DATASET_DEPENDENTS_IN_OBJ_MAP_TABLE_BY_NAME,
               dataPlatform,
               path,
               child_path);
     }
     return null;
   }

}
