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
package wherehows.common.utils;

public class PreparedStatementUtil {

  /**
   * prepare SQL insert template with column names and placeholders, 'INSERT INTO table(`a`,`b`) VALUES (?,?)'
   * @param tableName
   * @param columnNames String[]
   * @return SQL String
   */
  public static String prepareInsertTemplateWithColumn(String tableName, String[] columnNames) {
    return prepareInsertTemplateWithColumn("INSERT", tableName, columnNames);
  }

  /**
   * prepare SQL insert template with column names and placeholders, 'INSERT/REPLACE INTO table(`a`,`b`) VALUES (?,?)'
   * @param action INSERT or REPLACE
   * @param tableName
   * @param columnNames
   * @return
   */
  public static String prepareInsertTemplateWithColumn(String action, String tableName, String[] columnNames) {
    return action + " INTO " + tableName + "(`" + String.join("`,`", columnNames) + "`) VALUES " + generatePlaceholder(
        columnNames.length);
  }

  /**
   * prepare SQL insert template with placeholders, 'INSERT INTO table VALUES (?,?,?)'
   * @param tableName
   * @param columnNum int
   * @return SQL String
   */
  public static String prepareInsertTemplateWithoutColumn(String tableName, int columnNum) {
    return "INSERT INTO " + tableName + " VALUES " + generatePlaceholder(columnNum);
  }

  /**
   * prepare SQL update template with placeholders: "UPDATE table SET a=?, b=? WHERE c=? AND d=?"
   * @param tableName
   * @param columnNames String[] fields to be assigned/updated
   * @param conditions String[] condition fields
   * @return
   */
  public static String prepareUpdateTemplateWithColumn(String tableName, String[] columnNames, String[] conditions) {
    return "UPDATE " + tableName + " SET " + generateAssignmentFields(columnNames, ", ") + " WHERE "
        + generateAssignmentFields(conditions, " AND ");
  }

  /**
   * generate assignment fields string: `a`=?, `b`=? or `a`=? AND `b`=?
   * @param fields String[]
   * @param separator String
   * @return
   */
  public static String generateAssignmentFields(String[] fields, String separator) {
    String[] fieldAssignment = new String[fields.length];
    for (int i = 0; i < fields.length; i++) {
      fieldAssignment[i] = "`" + fields[i] + "`=?";
    }
    return String.join(separator, fieldAssignment);
  }

  /**
   * generate placeholder string (?,?,?)
   * @param columnNum number of placeholders
   * @return
   */
  public static String generatePlaceholder(int columnNum) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < columnNum; i++) {
      sb.append("?,");
    }
    return "(" + sb.substring(0, sb.length() - 1) + ")";
  }
}
