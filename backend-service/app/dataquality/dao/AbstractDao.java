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
package dataquality.dao;

import dataquality.DataQualityEngine;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;


/**
 * Created by zechen on 8/13/15.
 */
public abstract class AbstractDao {

  public static JdbcTemplate dqJdbc = DataQualityEngine.getInstance().getDataQualityJdbcTemplate();
  public static NamedParameterJdbcTemplate dqNamedJdbc = DataQualityEngine.getInstance().getDataQualityNamedJdbcTemplate();

  public static JdbcTemplate tdJdbc = DataQualityEngine.getInstance().getTeradataJdbcTemplate();
  public static NamedParameterJdbcTemplate tdNamedJdbc = DataQualityEngine.getInstance().getTeradataNamedJdbcTemplate();

}
