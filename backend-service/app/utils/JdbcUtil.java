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
package utils;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import play.db.DB;

import javax.sql.DataSource;
import java.util.Map;


/**
 * Created by zechen on 9/4/15.
 */
public class JdbcUtil {

  public static DataSource dataSource = DB.getDataSource("wherehows");
  public static JdbcTemplate wherehowsJdbcTemplate = new JdbcTemplate(dataSource);
  public static NamedParameterJdbcTemplate wherehowsNamedJdbcTemplate =
    new NamedParameterJdbcTemplate(DB.getDataSource("wherehows"));

  public static KeyHolder insertRow(String sql, Map<String, ?> params) {
    KeyHolder keyHolder = new GeneratedKeyHolder();
    SqlParameterSource parameterSource = new MapSqlParameterSource(params);
    wherehowsNamedJdbcTemplate.update(sql, parameterSource, keyHolder);
    return keyHolder;
  }
}
