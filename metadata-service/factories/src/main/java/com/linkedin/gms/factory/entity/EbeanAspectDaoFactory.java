package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import io.ebean.EbeanServer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;


@Configuration
public class EbeanAspectDaoFactory {

  @Bean(name = "ebeanAspectDao")
  @DependsOn({"gmsEbeanServiceConfig"})
  @ConditionalOnProperty(name = "entityService.impl", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected EbeanAspectDao createInstance(EbeanServer server) {
    return new EbeanAspectDao(server);
  }
}
