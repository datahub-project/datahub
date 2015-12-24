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
package dataquality;

import java.util.Map;
import javax.sql.DataSource;
import models.daos.EtlJobPropertyDao;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import play.Logger;


/**
 * Singleton class of the data quality engine, including all db connections
 * Created by zechen on 5/14/15.
 */
public class DataQualityEngine {
  private static final DataQualityEngine INSTANCE = new DataQualityEngine();

  private JdbcTemplate dataQualityJdbcTemplate;
  private NamedParameterJdbcTemplate dataQualityNamedJdbcTemplate;

  private JdbcTemplate teradataJdbcTemplate;
  private NamedParameterJdbcTemplate teradataNamedJdbcTemplate;

  public static final String DQ_JDBC_URL_KEY = "dataquality.db.jdbc.url";
  public static final String DQ_JDBC_USER_KEY = "dataquality.db.username";
  public static final String DQ_JDBC_PASSWORD_KEY = "dataquality.db.password";
  public static final String TD_JDBC_URL_KEY = "teradata.db.jdbc.url";
  public static final String TD_JDBC_USER_KEY = "teradata.db.username";
  public static final String TD_JDBC_PASSWORD_KEY = "teradata.db.password";

  private DataQualityEngine() {
    try {
      // get data quality database connection
      Map<String, String> dqProps = EtlJobPropertyDao.getWherehowsPropertiesByGroup("dataquality");
      String dqJdbcUrl = dqProps.get(DQ_JDBC_URL_KEY);
      String dqUser = dqProps.get(DQ_JDBC_USER_KEY);
      String dqPwd = dqProps.get(DQ_JDBC_PASSWORD_KEY);
      DataSource dqDs = new DriverManagerDataSource(dqJdbcUrl, dqUser, dqPwd);
      this.dataQualityJdbcTemplate = new JdbcTemplate(dqDs);
      this.dataQualityNamedJdbcTemplate = new NamedParameterJdbcTemplate(dqDs);

      // get teradata database connection
      String tdJdbcUrl = dqProps.get(TD_JDBC_URL_KEY);
      String tdUser = dqProps.get(TD_JDBC_USER_KEY);
      String tdPwd = dqProps.get(TD_JDBC_PASSWORD_KEY);
      DataSource tdDs = new DriverManagerDataSource(tdJdbcUrl, tdUser, tdPwd);
      this.teradataJdbcTemplate = new JdbcTemplate(tdDs);
      this.teradataNamedJdbcTemplate = new NamedParameterJdbcTemplate(tdDs);
    } catch (Exception e) {
      Logger.error("initialize data quality engine failed: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
  }

  public static DataQualityEngine getInstance() {
    return INSTANCE;
  }

  public JdbcTemplate getDataQualityJdbcTemplate() {
    return dataQualityJdbcTemplate;
  }

  public NamedParameterJdbcTemplate getDataQualityNamedJdbcTemplate() {
    return dataQualityNamedJdbcTemplate;
  }

  public JdbcTemplate getTeradataJdbcTemplate() {
    return teradataJdbcTemplate;
  }

  public NamedParameterJdbcTemplate getTeradataNamedJdbcTemplate() {
    return teradataNamedJdbcTemplate;
  }

}
