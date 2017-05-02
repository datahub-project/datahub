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
package dao;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class AbstractMySQLOpenSourceDAO
{
    private static String MYSQL_DB_WHEREHOWS_OPENSOURCE = "wherehows_opensource_mysql";

    private static JdbcTemplate jdbcTemplate = new JdbcTemplate(DataSource.getDataSource(MYSQL_DB_WHEREHOWS_OPENSOURCE));

    private static NamedParameterJdbcTemplate namedJdbcTemplate =
        new NamedParameterJdbcTemplate(DataSource.getDataSource(MYSQL_DB_WHEREHOWS_OPENSOURCE));

    protected static JdbcTemplate getJdbcTemplate()
  {
    return jdbcTemplate;
  }

    protected static NamedParameterJdbcTemplate getNamedParameterJdbcTemplate()
  {
    return namedJdbcTemplate;
  }

}
