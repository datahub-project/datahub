/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.hibernate.hikaricp.internal.HikariCPConnectionProvider;
import play.Logger;
import play.Play;
import play.mvc.Controller;
import play.mvc.Result;
import wherehows.dao.DaoFactory;


public class Application extends Controller {

    private static final String WHZ_APP_ENV = System.getenv("WHZ_APP_HOME");
    private static final String DB_WHEREHOWS_URL = Play.application().configuration().getString("db.wherehows.url");
    private static final String DB_WHEREHOWS_USERNAME =
            Play.application().configuration().getString("db.wherehows.username");
    private static final String DB_WHEREHOWS_PASSWORD =
            Play.application().configuration().getString("db.wherehows.password");

    public static final EntityManagerFactory entityManagerFactory;

    static {
        Map<String, String> properties = new HashMap<>();
        properties.put("hibernate.connection.provider_class", HikariCPConnectionProvider.class.getName());
        properties.put("hibernate.hikari.dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        properties.put("hibernate.hikari.dataSource.url", DB_WHEREHOWS_URL);
        properties.put("hibernate.hikari.dataSource.user", DB_WHEREHOWS_USERNAME);
        properties.put("hibernate.hikari.dataSource.password", DB_WHEREHOWS_PASSWORD);
        properties.put("hibernate.hikari.dataSource.cachePrepStmts", "true");
        properties.put("hibernate.hikari.dataSource.prepStmtCacheSize", "250");
        properties.put("hibernate.hikari.dataSource.prepStmtCacheSqlLimit", "2048");
        properties.put("hibernate.hikari.minimumIdle", "5");
        properties.put("hibernate.hikari.maximumPoolSize", "10");
        properties.put("hibernate.hikari.idleTimeout", "30000");
        properties.put("hibernate.show_sql", "false");
        properties.put("hibernate.dialect", "MySQL5");
        properties.put("hibernate.jdbc.batch_size", "100");
        properties.put("hibernate.order_inserts", "true");
        properties.put("hibernate.order_updates", "true");
        entityManagerFactory = Persistence.createEntityManagerFactory("default", properties);
    }

    public static final DaoFactory daoFactory = createDaoFactory();

    private static DaoFactory createDaoFactory() {
        try {
            String className = Play.application().configuration().getString("dao.entityManagerFactory.class", DaoFactory.class.getCanonicalName());
            Class factoryClass = Class.forName(className);
            Constructor<? extends DaoFactory> ctor = factoryClass.getConstructor(EntityManagerFactory.class);
            return ctor.newInstance(entityManagerFactory);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Result index() {
        return ok("TEST");
    }

    public static Result healthcheck() {
        return ok("GOOD");
    }

    public static Result printDeps() {
        String libPath = WHZ_APP_ENV + "/lib";
        String commitFile = WHZ_APP_ENV + "/commit";
        String libraries = "";
        String commit = "";

        if (WHZ_APP_ENV == null) {
            return ok("WHZ_APP_HOME environmental variable not defined");
        }

        try {
            BufferedReader br = new BufferedReader(new FileReader(commitFile));
            commit = br.readLine();
        } catch (IOException ioe) {
            Logger.error("Error while reading commit file. Error message: " +
                    ioe.getMessage());
        }

        //get all the files from a directory
        File directory = new File(libPath);
        for (File file : directory.listFiles()) {
            if (file.isFile()) {
                libraries += file.getName() + "\n";
            }
        }

        return ok("commit: " + commit + "\n" + libraries);
    }
}
