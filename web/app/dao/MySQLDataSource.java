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

import org.apache.commons.lang3.StringUtils;
import play.Play;

import java.util.Objects;

/**
 * @author SunZhaonan
 * @author R.Kluszczynski
 */
public class MySQLDataSource extends DataSource
{
    public static String DatabaseType = "mysql";
    public static String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    public static String DATABASE_WHEREHOWS_OPENSOURCE = "wherehows_opensource_mysql";

    private static String DATABASE_WHEREHOWS_OPENSOURCE_DRIVER_KEY = "database.opensource.driver";
    private static String DATABASE_WHEREHOWS_OPENSOURCE_USERNAME_KEY = "database.opensource.username";
    private static String DATABASE_WHEREHOWS_OPENSOURCE_PASSWORD_KEY = "database.opensource.password";
    private static String DATABASE_WHEREHOWS_OPENSOURCE_URL_KEY = "database.opensource.url";

    @Override
    public String getType()
    {
        return DatabaseType;
    }

    public MySQLDataSource(String identifier)
    {
        setDriverClass(getConfigurationValue(DATABASE_WHEREHOWS_OPENSOURCE_DRIVER_KEY, MYSQL_DRIVER_CLASS));
        if (StringUtils.isNotBlank(identifier) && identifier.equalsIgnoreCase(DATABASE_WHEREHOWS_OPENSOURCE))
        {
            setUsername(getConfigurationValue(DATABASE_WHEREHOWS_OPENSOURCE_USERNAME_KEY, "wherehows"));
            setPassword(getConfigurationValue(DATABASE_WHEREHOWS_OPENSOURCE_PASSWORD_KEY, "wherehows"));
            setJdbcUrl(getConfigurationValue(DATABASE_WHEREHOWS_OPENSOURCE_URL_KEY, "jdbc:mysql://localhost/wherehows"));
        }
        setIdleConnectionTestPeriodInMinutes(1);
        setIdleMaxAgeInMinutes(1);
        setMaxConnectionsPerPartition(10);
        setMinConnectionsPerPartition(5);
        setPartitionCount(3);
        setAcquireIncrement(5);
        setStatementsCacheSize(100);
    }

    private String getConfigurationValue(String configurationKey, String defaultValue)
    {
        final String configurationValue = Play.application().configuration().getString(configurationKey);
        if (Objects.isNull(configurationValue)) {
            return defaultValue;
        }
        return configurationValue;
    }
}
