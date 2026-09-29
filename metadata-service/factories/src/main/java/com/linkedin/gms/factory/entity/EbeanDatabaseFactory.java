package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.queue.ebean.EbeanPgQueueTopic;
import io.ebean.Database;
import io.ebean.config.DatabaseConfig;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@Slf4j
public class EbeanDatabaseFactory {
  public static final String EBEAN_MODEL_PACKAGE = EbeanAspectV2.class.getPackage().getName();

  public static final String EBEAN_QUEUE_MODEL_PACKAGE =
      EbeanPgQueueTopic.class.getPackage().getName();

  /**
   * Main entity-store Ebean server. Marked {@link Primary} so consumers that inject a bare {@link
   * Database} (or {@code ObjectProvider<Database>}) without a qualifier still resolve to the
   * entity-store pool — secondary servers like {@code pgQueueEbeanServer} / {@code
   * pgGraphEbeanServer} must be requested by qualifier.
   */
  @Bean("ebeanServer")
  @Primary
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected Database createServer(
      @Qualifier("gmsEbeanDatabaseConfig") DatabaseConfig serverConfig) {
    // Make sure that the serverConfig includes the package that contains DAO's Ebean model.
    if (!serverConfig.getPackages().contains(EBEAN_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_MODEL_PACKAGE);
    }
    if (!serverConfig.getPackages().contains(EBEAN_QUEUE_MODEL_PACKAGE)) {
      serverConfig.getPackages().add(EBEAN_QUEUE_MODEL_PACKAGE);
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
