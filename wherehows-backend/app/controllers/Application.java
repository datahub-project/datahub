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
import java.util.Map;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

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

  public static final EntityManagerFactory entityManagerFactory;

  static {
    entityManagerFactory = ConnectionPoolProperties.builder()
        .providerClass(HikariCPConnectionProvider.class.getName())
        .dataSourceClassName(WHZ_DB_DSCLASSNAME)
        .dataSourceURL(DB_WHEREHOWS_URL)
        .dataSourceUser(DB_WHEREHOWS_USERNAME)
        .dataSourcePassword(DB_WHEREHOWS_PASSWORD)
        .dialect(DB_WHEREHOWS_DIALECT)
        .build()
        .buildEntityManagerFactory();
  }

  public static final DaoFactory daoFactory = createDaoFactory();

  private static DaoFactory createDaoFactory() {
    try {
      String className = Play.application()
          .configuration()
          .getString("dao.entityManagerFactory.class", DaoFactory.class.getCanonicalName());
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
      Logger.error("Error while reading commit file. Error message: " + ioe.getMessage());
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
