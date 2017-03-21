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

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Created by nazhang on 1/6/17.
 */
public class JdbcConnection {

  // public static DataSource dataSource = DB.getDataSource("wherehows"); // this is the contructor parameter
  // public DataSource dataSource = getDataSource(); // this is the contructor parameter

  // practice creating data source using JDBC standard instead of play
  /*
  public JdbcTemplate wherehowsJdbcTemplate = new JdbcTemplate(dataSource);
  public NamedParameterJdbcTemplate wherehowsNamedJdbcTemplate =
      new NamedParameterJdbcTemplate(dataSource);
  */

  public static NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(String iDriverClassName, String iUrl, String iDbUserName, String iDbPassword)
  {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName(iDriverClassName);
    dataSource.setUrl(iUrl);
    dataSource.setUsername(iDbUserName);
    dataSource.setPassword(iDbPassword);
    return new NamedParameterJdbcTemplate(dataSource);
  }

}
