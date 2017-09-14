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
package controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import javax.persistence.EntityManagerFactory;

import org.hibernate.hikaricp.internal.HikariCPConnectionProvider;
import play.Logger;
import play.Play;
import play.mvc.Controller;
import play.mvc.Result;
import wherehows.dao.ConnectionPoolProperties;
import wherehows.dao.DaoFactory;


public class Application extends Controller {

  private static final String WHZ_APP_ENV = System.getenv("WHZ_APP_HOME");
  private static final String DB_WHEREHOWS_URL = Play.application().configuration().getString("db.wherehows.url");
  private static final String WHZ_DB_DSCLASSNAME =
      Play.application().configuration().getString("hikaricp.dataSourceClassName");
  private static final String DB_WHEREHOWS_USERNAME =
      Play.application().configuration().getString("db.wherehows.username");
  private static final String DB_WHEREHOWS_PASSWORD =
      Play.application().configuration().getString("db.wherehows.password");
  private static final String DB_WHEREHOWS_DIALECT = Play.application().configuration().getString("hikaricp.dialect");
  private static final String DAO_FACTORY_CLASS =
      Play.application().configuration().getString("dao.factory.class", DaoFactory.class.getCanonicalName());

  private static final EntityManagerFactory ENTITY_MANAGER_FACTORY = ConnectionPoolProperties.builder()
      .providerClass(HikariCPConnectionProvider.class.getName())
      .dataSourceClassName(WHZ_DB_DSCLASSNAME)
      .dataSourceURL(DB_WHEREHOWS_URL)
      .dataSourceUser(DB_WHEREHOWS_USERNAME)
      .dataSourcePassword(DB_WHEREHOWS_PASSWORD)
      .dialect(DB_WHEREHOWS_DIALECT)
      .build()
      .buildEntityManagerFactory();

  public static final DaoFactory DAO_FACTORY = createDaoFactory();

  private static DaoFactory createDaoFactory() {
    try {
      Logger.info("Creating DAO factory: " + DAO_FACTORY_CLASS);
      Class factoryClass = Class.forName(DAO_FACTORY_CLASS);
      Constructor<? extends DaoFactory> ctor = factoryClass.getConstructor(EntityManagerFactory.class);
      return ctor.newInstance(ENTITY_MANAGER_FACTORY);
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
    String commit = "";

    if (WHZ_APP_ENV == null) {
      return ok("WHZ_APP_HOME environmental variable not defined");
    }

    try {
      commit = new BufferedReader(new FileReader(commitFile)).readLine();
    } catch (IOException ioe) {
      Logger.error("Error while reading commit file. Error message: " + ioe.getMessage());
    }

    //get all the files from /libs directory
    File directory = new File(libPath);
    StringBuilder sb = new StringBuilder();
    if (directory.listFiles() != null) {
      for (File file : directory.listFiles()) {
        if (file.isFile()) {
          sb.append(file.getName()).append("\n");
        }
      }
    }

    return ok("commit: " + commit + "\n" + "libraries: " + sb.toString());
  }
}
