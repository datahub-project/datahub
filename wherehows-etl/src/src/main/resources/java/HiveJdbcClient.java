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

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zechen on 11/18/15.
 */

public class HiveJdbcClient {
  private static String hiveJdbcDriverName = "org.apache.hive.jdbc.HiveDriver";
  private static final char SEPR = 0x001A;
  private static final String DATASET_URN_PREFIX = "hdfs:///";

  public static void main(String[] args) throws Exception {
    try {
      Class.forName(hiveJdbcDriverName);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }

    try {
      // Embedded mode
      Connection conn = DriverManager.getConnection("jdbc:hive2://", "", "");
      Statement stmt = conn.createStatement();
      String query = new String(Files.readAllBytes(Paths.get(args[0])));
      // Even thought the query is loaded from a file
      // but hive jdbc executeQuery method only support a single SELECT query
      ResultSet resultSet = stmt.executeQuery(query);
      int count = 0;
      OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(args[1]));
      while (resultSet.next()) {
        Long sourceTime = resultSet.getLong(1);
        String cluster = resultSet.getString(2);
        String datasetPath = resultSet.getString(3);
        String ownerUrns = resultSet.getString(4);
        String[] datasets = datasetPath.trim().split("\\s*,\\s*");
        String[] owners = findUsers(ownerUrns);

        for (String d : datasets) {
          int sortId = 0;
          for (String o : owners) {
            out.write(DATASET_URN_PREFIX + findUrn(d));
            out.write(SEPR);
            int idx = o.lastIndexOf(':');
            String prefix = o.substring(0, idx).trim();
            String owner = o.substring(idx + 1).trim();
            out.write(owner);
            out.write(SEPR);
            out.write(String.valueOf(sortId));
            out.write(SEPR);
            out.write(prefix);
            out.write(SEPR);
            out.write(cluster);
            out.write(SEPR);
            out.write(sourceTime.toString());
            out.write('\n');
            sortId++;
          }
        }
        count++;
      }
      out.flush();
      out.close();
      System.out.println("total count: " + count);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("???????hive jdbc failed!!!!!!");
    }
  }

  public static String[] findUsers(String ownerUrns) {
    Pattern pattern = Pattern.compile("\\[(.*?)\\]");
    Matcher m = pattern.matcher(ownerUrns);
    if (m.find()) {
      return m.group(1).trim().split("\\s*,\\s*");
    }
    return new String[0];
  }

  /**
   * get rid of the case that has host name and port
   * @param datasetPath
   * @return
   */
  public static String findUrn(String datasetPath) {
    // dealing with path that contains host name, or forgot to start with '/'
    Pattern pattern = Pattern.compile("([^/]/|^/)[^/]");
    Matcher m = pattern.matcher(datasetPath);
    int begin = 0;
    if (m.find() && (datasetPath.contains("://") || datasetPath.startsWith("/"))) {
      begin = m.end() - 1;
    }

    // dealing with path that have additional '/' or '/*/*';
    pattern = Pattern.compile("(/|\\*)*(.avro)*$");
    m = pattern.matcher(datasetPath);
    int end = datasetPath.length();
    if (m.find()) {
      end = m.start();
    }
    return datasetPath.substring(begin, end);
  }
}
