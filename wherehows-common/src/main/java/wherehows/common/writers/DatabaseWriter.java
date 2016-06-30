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
}
