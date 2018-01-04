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
package wherehows.main;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import javax.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.hikaricp.internal.HikariCPConnectionProvider;
import wherehows.actors.KafkaClientMaster;
import wherehows.common.utils.ProcessUtil;
import wherehows.dao.ConnectionPoolProperties;
import wherehows.dao.DaoFactory;


@Slf4j
public class ApplicationStart {

  private static final String PID_FILE_PATH_KEY = "pidfile.path";

  private static final Config config = ConfigFactory.load();
  private static final String DB_WHEREHOWS_URL = config.getString("db.wherehows.url");
  private static final String WHZ_DB_DSCLASSNAME = config.getString("hikaricp.dataSourceClassName");
  private static final String DB_WHEREHOWS_USERNAME = config.getString("db.wherehows.username");
  private static final String DB_WHEREHOWS_PASSWORD = config.getString("db.wherehows.password");
  private static final String DB_WHEREHOWS_DIALECT = config.getString("hikaricp.dialect");
  private static final String DAO_FACTORY_CLASS =
      config.hasPath("dao.factory.class") ? config.getString("dao.factory.class") : DaoFactory.class.getCanonicalName();

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
      log.info("Creating DAO factory: " + DAO_FACTORY_CLASS);
      Class factoryClass = Class.forName(DAO_FACTORY_CLASS);
      Constructor<? extends DaoFactory> ctor = factoryClass.getConstructor(EntityManagerFactory.class);
      return ctor.newInstance(ENTITY_MANAGER_FACTORY);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void writeProcessId() {
    String pidFile = System.getProperty(PID_FILE_PATH_KEY);
    if (pidFile == null) {
      return;
    }
    log.info("Writing PID to " + pidFile);

    try (PrintWriter out = new PrintWriter(pidFile, "UTF-8")) {
      out.println(ProcessUtil.getCurrentProcessId());
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
      log.error("Unable to write to " + pidFile);
    }
  }

  public static void main(String[] args) {
    log.info("Starting WhereHows KAFKA Consumer Service");

    writeProcessId();

    final ActorSystem actorSystem = ActorSystem.create("KAFKA");

    String kafkaJobDir = config.getString("kafka.jobs.dir");
    actorSystem.actorOf(Props.create(KafkaClientMaster.class, kafkaJobDir), "KafkaMaster");
  }
}
