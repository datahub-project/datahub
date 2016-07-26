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
package wherehows.common.writers;

import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import wherehows.common.schemas.AbstractRecord;
import wherehows.common.schemas.Record;


/**
 * Created by zsun on 8/20/15.
 */
public class DatabaseWriter extends Writer {
  JdbcTemplate jdbcTemplate;
  String tableName;
  private static final Logger logger = LoggerFactory.getLogger(DatabaseWriter.class);

  public DatabaseWriter(JdbcTemplate jdbcTemplate, String tableName) {
    this.jdbcTemplate = jdbcTemplate;
    this.tableName = tableName;
  }

  public DatabaseWriter(DataSource dataSource, String tableName) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.tableName = tableName;
  }


  public DatabaseWriter(String connectionUrl, String tableName) throws SQLException {
    DriverManagerDataSource dataSource = new DriverManagerDataSource(connectionUrl);
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.tableName = tableName;
  }

  public synchronized void update(String setValues, String urn) {

    StringBuilder sb = new StringBuilder();
    sb.append("UPDATE " + this.tableName +" SET "+ setValues +" WHERE urn = '"+urn+"'");
    try {
      this.jdbcTemplate.execute(sb.toString());
    } catch (DataAccessException e) {
      logger.error("UPDATE statement have error : " + sb.toString() + e);
    }
  }

  //TODO: this insert sql is too ambitious, need add column names
  @Override
  public synchronized boolean flush()
    throws SQLException {
    if (records.size() == 0) {
      return false;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO " + this.tableName + " VALUES ");
    for (Record r : this.records) {
      sb.append("(" + r.toDatabaseValue() + "),");
    }
    sb.deleteCharAt(sb.length() - 1);

    logger.debug("In databaseWriter : " + sb.toString());

    try {
      this.jdbcTemplate.execute(sb.toString());
    } catch (DataAccessException e) {
      logger.error("This statement have error : " + sb.toString());
      this.records.clear(); // need to recover the records.
    }
    this.records.clear();
    return false;
  }

  @Override
  public void close()
    throws SQLException {
    this.flush();
  }


  /**
   * use JDBC template PreparedStatement to insert records
   * require record to have getAllValues() and optional getDbColumnNames() method
   * @return boolean if the insert is successful
   * @throws SQLException
   */
  public boolean insert()
      throws SQLException {
    if (records.size() == 0 || !(records.get(0) instanceof AbstractRecord)) {
      logger.debug("DatabaseWriter no record to insert or unknown record Class.");
      return false;
    }

    final AbstractRecord record0 = (AbstractRecord) records.get(0);
    final String[] columnNames = record0.getDbColumnNames();
    final String sql = (columnNames != null) ? prepareInsertTemplateWithColumn(columnNames)
        : prepareInsertTemplateWithoutColumn(record0.getAllFields().length);
    //logger.debug("DatabaseWriter template for " + record0.getClass() + " : " + sql);

    for (final Record record : records) {
      try {
        jdbcTemplate.update(sql, ((AbstractRecord) record).getAllValues());

        /* jdbcTemplate.update(sql, new PreparedStatementSetter() {
          @Override
          public void setValues(PreparedStatement ps)
              throws SQLException {
            record.setPreparedStatementValues(ps);
          }
        }); */
      } catch (IllegalAccessException | DataAccessException ae) {
        logger.error("DatabaseWriter insert error: " + ae);
      }
    }
    records.clear();
    return true;
  }


  /**
   * prepare SQL insert template with column names and placeholders, 'INSERT INTO table(`a`,`b`) VALUES (?,?)'
   * @param columnNames String[]
   * @return SQL String
   */
  private String prepareInsertTemplateWithColumn(String[] columnNames) {
    return "INSERT INTO " + tableName + "(`" + String.join("`,`", columnNames) + "`) VALUES "
        + generatePlaceholder(columnNames.length);
  }

  /**
   * prepare SQL insert template with placeholders, 'INSERT INTO table VALUES (?,?,?)'
   * @param columnNum int
   * @return SQL String
   */
  private String prepareInsertTemplateWithoutColumn(int columnNum) {
    return "INSERT INTO " + tableName + " VALUES " + generatePlaceholder(columnNum);
  }

  /**
   * generate placeholder string (?,?,?)
   * @param columnNum number of placeholders
   * @return
   */
  private String generatePlaceholder(int columnNum) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < columnNum; i++) {
      sb.append("?,");
    }
    return "(" + sb.substring(0, sb.length() - 1) + ")";
  }
}
