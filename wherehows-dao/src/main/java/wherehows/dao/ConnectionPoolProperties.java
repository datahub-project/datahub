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
package wherehows.dao;

import java.util.HashMap;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import lombok.Builder;


@Builder
public class ConnectionPoolProperties {
  private final String providerClass;
  private final String dataSourceClassName;
  private final String dataSourceURL;
  private final String dataSourceUser;
  private final String dataSourcePassword;
  @Builder.Default
  private final String minimumIdle = "10";
  @Builder.Default
  private final String maximumPoolSize = "20";
  @Builder.Default
  private final String idleTimeout = "30000";
  @Builder.Default
  private final String showSQL = "false";
  private final String dialect;
  @Builder.Default
  private final String jdbcBatchSize = "100";
  @Builder.Default
  private final String orderInserts = "true";
  @Builder.Default
  private final String orderUpdates = "true";

  public EntityManagerFactory buildEntityManagerFactory() {
    Map<String, String> properties = new HashMap<>();
    properties.put("hibernate.connection.provider_class", providerClass);
    properties.put("hibernate.hikari.dataSourceClassName", dataSourceClassName);
    properties.put("hibernate.hikari.dataSource.url", dataSourceURL);
    properties.put("hibernate.hikari.dataSource.user", dataSourceUser);
    properties.put("hibernate.hikari.dataSource.password", dataSourcePassword);
    properties.put("hibernate.hikari.minimumIdle", minimumIdle);
    properties.put("hibernate.hikari.maximumPoolSize", maximumPoolSize);
    properties.put("hibernate.hikari.idleTimeout", idleTimeout);
    properties.put("hibernate.show_sql", showSQL);
    properties.put("hibernate.dialect", dialect);
    properties.put("hibernate.jdbc.batch_size", jdbcBatchSize);
    properties.put("hibernate.order_inserts", orderInserts);
    properties.put("hibernate.order_updates", orderUpdates);

    return Persistence.createEntityManagerFactory("default", properties);
  }
}
