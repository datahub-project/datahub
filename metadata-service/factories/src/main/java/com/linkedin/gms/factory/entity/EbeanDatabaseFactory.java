/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import io.ebean.Database;
import io.ebean.config.DatabaseConfig;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class EbeanDatabaseFactory {
  public static final String EBEAN_MODEL_PACKAGE = EbeanAspectV2.class.getPackage().getName();

  @Bean("ebeanServer")
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected Database createServer(
      @Qualifier("gmsEbeanDatabaseConfig") DatabaseConfig serverConfig) {
    // Make sure that the serverConfig includes the package that contains DAO's Ebean model.
    if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
    }
    // TODO: Consider supporting SCSI
    try {
      return io.ebean.DatabaseFactory.create(serverConfig);
    } catch (NullPointerException ne) {
      log.error("Failed to connect to the server. Is it up?");
      throw ne;
    }
  }
}
