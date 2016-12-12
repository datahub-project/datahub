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
package metadata.etl.lineage;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import wherehows.common.DatasetPath;
import wherehows.common.schemas.AzkabanJobExecRecord;
import wherehows.common.schemas.LineageRecord;


/**
 * Created by zsun on 9/8/15.
 */
public class AzLogParser {

  static List<LogLineagePattern> logLineagePatterns;
  static List<String> logHadoopIdPatterns;

  /**
   * Parse the hadoop job id from the log.
   * @param log
   * @return A list of hadoop job id
   */
  public static Set<String> getHadoopJobIdFromLog(String log) {
    // build a single pattern so only parse once.
    StringBuilder patterns = new StringBuilder();
    for (String s : logHadoopIdPatterns) {
      patterns.append(s).append("|");
    }
    patterns.deleteCharAt(patterns.length() - 1);
    Pattern hadoopJobIdPattern = Pattern.compile(patterns.toString());
    Matcher matcher = hadoopJobIdPattern.matcher(log);
    Set<String> listMatches = new HashSet<>();

    while (matcher.find()) {
      for (int i = 1; i <= logHadoopIdPatterns.size(); i++) {
        if (matcher.group(i) != null) {
          listMatches.add(matcher.group(i));
        }
      }
    }
    return listMatches;
  }

  /**
   * initialize, download the regex info into cache
   */
  public synchronized static void initialize(Connection conn)
    throws SQLException {
    if (logHadoopIdPatterns != null && logLineagePatterns != null) {
      return;
    }
    loadLineagePatterns(conn);
    loadHadoopIdPatterns(conn);
  }

  private static void loadLineagePatterns(Connection conn)
    throws SQLException {
    logLineagePatterns = new ArrayList<>();
    String cmd = "SELECT regex, database_type, database_name_index, dataset_index, operation_type, source_target_type, "
      + "record_count_index, record_byte_index, insert_count_index, insert_byte_index, "
      + "delete_count_index, delete_byte_index, update_count_index, update_byte_index FROM log_lineage_pattern";
    ResultSet resultSet = conn.createStatement().executeQuery(cmd);
    while (resultSet.next()) {
      LogLineagePattern logLineagePattern =
        new LogLineagePattern(resultSet.getString("regex"), resultSet.getString("database_type"),
          resultSet.getInt("database_name_index"), resultSet.getInt("dataset_index"),
          resultSet.getString("source_target_type"), resultSet.getString("operation_type"),
          resultSet.getInt("record_count_index"), resultSet.getInt("record_byte_index"),
          resultSet.getInt("insert_count_index"), resultSet.getInt("insert_byte_index"),
          resultSet.getInt("delete_count_index"), resultSet.getInt("delete_byte_index"),
          resultSet.getInt("update_count_index"), resultSet.getInt("update_byte_index"));
      logLineagePatterns.add(logLineagePattern);
    }
  }

  private static void loadHadoopIdPatterns(Connection conn)
    throws SQLException {
    logHadoopIdPatterns = new ArrayList<>();
    String cmd = "SELECT regex FROM log_reference_job_id_pattern WHERE is_active = 1";
    ResultSet resultSet = conn.createStatement().executeQuery(cmd);
    while (resultSet.next()) {
      logHadoopIdPatterns.add(resultSet.getString("regex"));
    }
  }

  /**
   * Mining lineage from Azkaban log
   * @param log azkaban log
   * @param azkabanJobExecRecord contain the job execution info to construct the result
   * @return
   */
  public static List<LineageRecord> getLineageFromLog(String log, AzkabanJobExecRecord azkabanJobExecRecord, Integer defaultDatabaseId) {

    List<LineageRecord> result = new ArrayList<>();
    Pattern typePattern = Pattern.compile("^(\\w+):/.*");
    String datasetType = "";

    for (LogLineagePattern patternObject : logLineagePatterns) {
      Pattern pattern = Pattern.compile(patternObject.regex);
      Matcher matcher = pattern.matcher(log);
      while (matcher.find()) {
        String datasetString = matcher.group(patternObject.datasetIndex);
        List<String> datasets = DatasetPath.separatedDataset(datasetString);
        for (String dataset : datasets) {
          if (patternObject.databaseNameIndex > 0) {
            // add the database name if it exists
            dataset = matcher.group(patternObject.databaseNameIndex) + "/" + dataset;
          }
          LineageRecord lineageRecord =
            new LineageRecord(azkabanJobExecRecord.getAppId(), azkabanJobExecRecord.getFlowExecId(),
              azkabanJobExecRecord.getJobName(), azkabanJobExecRecord.getJobExecId());

          Matcher typeMatcher = typePattern.matcher(dataset);
          if (typeMatcher.matches()) {
            datasetType = typeMatcher.group(1);
          } else {
            datasetType = "hdfs";
          }
          lineageRecord.setDatasetInfo(defaultDatabaseId, dataset, datasetType);

          long recordCount =
            (patternObject.recordCountIndex < 1) ? 0 : Long.valueOf(matcher.group(patternObject.recordCountIndex));
          long insertCount =
            (patternObject.insertCountIndex < 1) ? 0 : Long.valueOf(matcher.group(patternObject.insertCountIndex));
          long deleteCount =
            (patternObject.deleteCountIndex < 1) ? 0 : Long.valueOf(matcher.group(patternObject.deleteCountIndex));
          long updateCount =
            (patternObject.updateCountIndex < 1) ? 0 : Long.valueOf(matcher.group(patternObject.updateCountIndex));
          lineageRecord
            .setOperationInfo(patternObject.sourceTargetType, patternObject.operation, recordCount, insertCount,
              deleteCount, updateCount, azkabanJobExecRecord.getStartTime(), azkabanJobExecRecord.getEndTime(),
              azkabanJobExecRecord.getFlowPath());
          result.add(lineageRecord);
        }
      }
    }
    return result;
  }

  static class LogLineagePattern {
    String regex;
    String databaseType;
    int databaseNameIndex;
    int datasetIndex;
    String sourceTargetType;
    String operation;
    int recordCountIndex;
    int recordByteIndex;
    int insertCountIndex;
    int insertByteIndex;
    int deleteCountIndex;
    int deleteByteIndex;
    int updateCountIndex;
    int updateByteIndex;

    public LogLineagePattern(String regex, String databaseType, int databaseNameIndex, int datasetIndex,
      String sourceTargetType, String operation, int recordCountIndex, int recordByteIndex, int insertCountIndex,
      int insertByteIndex, int deleteCountIndex, int deleteByteIndex, int updateCountIndex, int updateByteIndex) {
      this.regex = regex;
      this.databaseType = databaseType;
      this.databaseNameIndex = databaseNameIndex;
      this.datasetIndex = datasetIndex;
      this.sourceTargetType = sourceTargetType;
      this.operation = operation;
      this.recordCountIndex = recordCountIndex;
      this.recordByteIndex = recordByteIndex;
      this.insertCountIndex = insertCountIndex;
      this.insertByteIndex = insertByteIndex;
      this.deleteCountIndex = deleteCountIndex;
      this.deleteByteIndex = deleteByteIndex;
      this.updateCountIndex = updateCountIndex;
      this.updateByteIndex = updateByteIndex;
    }
  }
}
