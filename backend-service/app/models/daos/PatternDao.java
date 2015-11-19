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
import org.springframework.jdbc.support.KeyHolder;
import utils.JdbcUtil;
import utils.JsonUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zechen on 10/30/15.
 */
public class PatternDao {
  public static final String GET_ALL_FILENAME_PATTERNS = "SELECT * FROM filename_pattern";

  public static final String INSERT_FILENAME_PATTERN = "INSERT INTO filename_pattern (regex) VALUES (:regex)";

  public static final String GET_ALL_DATASET_PARTITION_PATTERNS = "SELECT * FROM dataset_partition_layout_pattern";

  public static final String INSERT_DATASET_PARTITION_PATTERN = "INSERT INTO dataset_partition_layout_pattern (regex, mask, leading_path_index, partition_index, second_partition_index, sort_id, partition_pattern_group, comments) " +
    " VALUES (:regex, :mask, :leadingPathIndex, :partitionIndex, :secondPartitionIndex, :sortId, :partitionPatternGroup, :comments)";

  public static final String GET_ALL_LOG_LINEAGE_PATTERNS = "SELECT * FROM log_lineage_pattern";

  public static final String INSERT_LOG_LINEAGE_PATTERN = "INSERT INTO log_lineage_pattern (regex, database_type, operation_type, database_name_index, dataset_index, record_count_index, record_byte_index, insert_count_index, insert_byte_index, delete_count_index, delete_byte_index, update_count_index, update_byte_index, pattern_type, source_target_type, comments) " +
    " VALUES (:regex, :databaseType, :operationType, :databaseNameIndex, :datasetIndex, :recordCountIndex, :recordByteIndex, :insertCountIndex, :insertByteIndex, :deleteCountIndex, :deleteByteIndex, :updateCountIndex, :updateByteIndex, :patternType, :sourceTargetType, :comments)";

  public static final String GET_ALL_LOG_JOB_ID_PATTERNS = "SELECT * FROM log_reference_job_id_pattern";

  public static final String INSERT_LOG_JOB_ID_PATTERNS = "INSERT INTO log_reference_job_id_pattern(pattern_type, regex, reference_job_id_index, is_active, comments) " +
    " VALUES (:patternType, :regex, :referenceJobIdIndex, :isActive, :comments)";


  public static List<Map<String, Object>> getAllFileNamePatterns() throws Exception {
    return JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_ALL_FILENAME_PATTERNS);
  }

  public static int insertFilenamePattern(JsonNode filename) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("regex", JsonUtil.getJsonValue(filename, "regex", String.class));

    KeyHolder kh = JdbcUtil.insertRow(INSERT_FILENAME_PATTERN, params);
    return kh.getKey().intValue();
  }

  public static List<Map<String, Object>> getAllDatasetPartitionPatterns() throws Exception {
    return JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_ALL_DATASET_PARTITION_PATTERNS);
  }

  public static int insertDatasetPartitionPattern(JsonNode datasetPartitionPattern) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("regex", JsonUtil.getJsonValue(datasetPartitionPattern, "regex", String.class));
    params.put("mask", JsonUtil.getJsonValue(datasetPartitionPattern, "mask", String.class));
    params.put("leadingPathIndex", JsonUtil.getJsonValue(datasetPartitionPattern, "leading_path_index", Integer.class, 1));
    params.put("partitionIndex", JsonUtil.getJsonValue(datasetPartitionPattern, "partition_index", Integer.class, 2));
    params.put("sortId", JsonUtil.getJsonValue(datasetPartitionPattern, "sort_id", Integer.class));
    params.put("secondPartitionIndex", JsonUtil.getJsonValue(datasetPartitionPattern, "second_partition_index", Integer.class, null));
    params.put("partitionPatternGroup", JsonUtil.getJsonValue(datasetPartitionPattern, "partition_pattern_group", Integer.class, null));
    params.put("comments", JsonUtil.getJsonValue(datasetPartitionPattern, "comments", String.class, null));

    KeyHolder kh = JdbcUtil.insertRow(INSERT_DATASET_PARTITION_PATTERN, params);
    return kh.getKey().intValue();
  }



  public static List<Map<String, Object>> getAllLogLineagePattern() throws Exception {
    return JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_ALL_LOG_LINEAGE_PATTERNS);
  }

  public static int insertLogLineagePattern(JsonNode logLineagePattern) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("regex", JsonUtil.getJsonValue(logLineagePattern, "regex", String.class));
    params.put("databaseType", JsonUtil.getJsonValue(logLineagePattern, "database_type", String.class));
    params.put("operationType", JsonUtil.getJsonValue(logLineagePattern, "operation_type", String.class));
    params.put("patternType", JsonUtil.getJsonValue(logLineagePattern, "pattern_type", String.class, null));
    params.put("sourceTargetType", JsonUtil.getJsonValue(logLineagePattern, "source_target_type", String.class, null));
    params.put("databaseNameIndex", JsonUtil.getJsonValue(logLineagePattern, "database_name_index", Integer.class, null));
    params.put("datasetIndex", JsonUtil.getJsonValue(logLineagePattern, "dataset_index", Integer.class, null));
    params.put("recordCountIndex", JsonUtil.getJsonValue(logLineagePattern, "record_count_index", Integer.class, null));
    params.put("recordByteIndex", JsonUtil.getJsonValue(logLineagePattern, "record_byte_index", Integer.class, null));
    params.put("insertCountIndex", JsonUtil.getJsonValue(logLineagePattern, "insert_count_index", Integer.class, null));
    params.put("insertByteIndex", JsonUtil.getJsonValue(logLineagePattern, "insert_byte_index", Integer.class, null));
    params.put("deleteCountIndex", JsonUtil.getJsonValue(logLineagePattern, "delete_count_index", Integer.class, null));
    params.put("deleteByteIndex", JsonUtil.getJsonValue(logLineagePattern, "delete_byte_index", Integer.class, null));
    params.put("updateCountIndex", JsonUtil.getJsonValue(logLineagePattern, "update_count_index", Integer.class, null));
    params.put("updateByteIndex", JsonUtil.getJsonValue(logLineagePattern, "update_byte_index", Integer.class, null));
    params.put("comments", JsonUtil.getJsonValue(logLineagePattern, "comments", String.class, null));

    KeyHolder kh = JdbcUtil.insertRow(INSERT_LOG_LINEAGE_PATTERN, params);
    return kh.getKey().intValue();
  }


  public static List<Map<String, Object>> getAllLogJobIdPattern() throws Exception {
    return JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_ALL_LOG_JOB_ID_PATTERNS);
  }

  public static int insertLogJobIdPattern(JsonNode logJobIdPattern) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("regex", JsonUtil.getJsonValue(logJobIdPattern, "regex", String.class));
    params.put("patternType", JsonUtil.getJsonValue(logJobIdPattern, "pattern_type", String.class, null));
    params.put("referenceJobIdIndex", JsonUtil.getJsonValue(logJobIdPattern, "reference_job_id_index", Integer.class));
    params.put("isActive", JsonUtil.getJsonValue(logJobIdPattern, "is_active", Boolean.class, true));
    params.put("comments", JsonUtil.getJsonValue(logJobIdPattern, "comments", String.class, null));

    KeyHolder kh = JdbcUtil.insertRow(INSERT_LOG_JOB_ID_PATTERNS, params);
    return kh.getKey().intValue();
  }


}
