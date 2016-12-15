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
package wherehows.common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Singleton path analyzer
 * Created by zsun on 9/11/15.
 */
public class PathAnalyzer {
  public static PathAnalyzer instance = null;

  private static List<Layout> _layoutsInfo;
  private static List<String> _filePattherns;

  /**
   * Put the layout info from database into memory
   * @param connection
   * @throws SQLException
   */
  public synchronized static void initialize(Connection connection)
    throws SQLException {
    if (_layoutsInfo != null && _filePattherns != null) {
      return;
    }
    String layoutQuery = "SELECT * FROM dataset_partition_layout_pattern ORDER BY sort_id";
    ResultSet resultSet = connection.createStatement().executeQuery(layoutQuery);
    _layoutsInfo = new ArrayList<>();
    while (resultSet.next()) {
      Layout layout =
        new Layout(resultSet.getInt("layout_id"), resultSet.getString("regex"), resultSet.getString("mask"),
          resultSet.getInt("leading_path_index"), resultSet.getInt("partition_index"),
          resultSet.getString("partition_pattern_group"));
      _layoutsInfo.add(layout);
    }

    String fileQuery = "SELECT regex FROM filename_pattern";
    ResultSet fpRs = connection.createStatement().executeQuery(fileQuery);
    _filePattherns = new ArrayList<>();
    while (fpRs.next()) {
      _filePattherns.add(fpRs.getString("regex"));
    }
  }


  /**
   * Analyze the path and extract the abstract level info from path.
   * @param fullPath
   * @return
   */
  public static DatasetPath analyze(String fullPath) {
    assert _layoutsInfo != null;
    assert _filePattherns != null;
    DatasetPath datasetPath = new DatasetPath();
    datasetPath.fullPath = fullPath;

    // remove the "dalids://" or "hive://" header
    Pattern daliPattern = Pattern.compile("(dalids|hive):///(\\w.*/)?(\\w.*)(\\.|/)(\\w.*)$");
    Matcher daliMatcher = daliPattern.matcher(fullPath);
    if (daliMatcher.matches()) {
      fullPath = String.format( "/%s/%s", daliMatcher.group(3), daliMatcher.group(5) );
    }

    // remove the "hdfs://.../" header
    Pattern headerPattern = Pattern.compile("hdfs://.*:\\d{4}(/.*)");
    Matcher headerMatcher = headerPattern.matcher(fullPath);
    if (headerMatcher.matches()) {
      fullPath = headerMatcher.group(1);
    }

    // remove 'tmp' folder
    if (fullPath.startsWith("/tmp/") || fullPath.startsWith("/temp/")) {
      return null;
    }

    // sometimes the input is a file, sometimes the input is a directory,
    // If this is a file, need to remove the file partition
    // e.g. (.*)/part-\d+, (.*)/part-\d+\.avro, (.*)/part-m-\d+\.avro, (.*)/part-r-\d+\.avro
    for (String filePattern : _filePattherns) {
      Pattern p = Pattern.compile(filePattern);
      Matcher m = p.matcher(fullPath);
      if (m.matches()) {
        fullPath = m.group(1);
      }
    }

    for (Layout layoutRecord : _layoutsInfo) {
      Pattern p = Pattern.compile(layoutRecord.regex);
      Matcher m = p.matcher(fullPath);
      if (m.matches()) {
        datasetPath.abstractPath = m.group(layoutRecord.leadingPathIndex);
        datasetPath.layoutId = layoutRecord.layoutId;
        datasetPath.partitionStart = m.group(layoutRecord.partitionIndex);
        datasetPath.partitionEnd = m.group(layoutRecord.partitionIndex);
        datasetPath.partitionType = layoutRecord.partitionPatternGroup;
        // System.out.println("Matched, the abstract path is : " + datasetPath.abstractPath);
        break;
      }
    }

    // handle this case : tcp://hostname:port/tablename
    Pattern tcpHeaderPattern = Pattern.compile("tcp://.*:\\d{5}/(.*)");
    Matcher tcpheaderMatcher = tcpHeaderPattern.matcher(fullPath);
    if (tcpheaderMatcher.matches()) {
      datasetPath.abstractPath = tcpheaderMatcher.group(1);
    }

    // if nothing match
    if (datasetPath.abstractPath == null) {
      datasetPath.abstractPath = fullPath;
    }

    return datasetPath;
  }

  /**
   * Inner class for Layout.
   */
  static class Layout {
    int layoutId;
    String regex;
    String mask;
    int leadingPathIndex;
    int partitionIndex;
    String partitionPatternGroup;

    public Layout(int layoutId, String regex, String mask, int leadingPathIndex, int partitionIndex,
      String partitionPatternGroup) {
      this.layoutId = layoutId;
      this.regex = regex;
      this.mask = mask;
      this.leadingPathIndex = leadingPathIndex;
      this.partitionIndex = partitionIndex;
      this.partitionPatternGroup = partitionPatternGroup;
    }
  }
}
