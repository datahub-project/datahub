package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.applyretention.ApplyRetention;
import com.linkedin.metadata.entity.RetentionService;
import io.ebean.EbeanServer;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class ApplyRetentionConfig {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "applyRetention")
  @DependsOn({"ebeanServer", "retentionService"})
  @Nonnull
  public ApplyRetention createInstance() {
    final EbeanServer ebeanServer = applicationContext.getBean(EbeanServer.class);
    final RetentionService retentionService = applicationContext.getBean(RetentionService.class);

    return new ApplyRetention(ebeanServer, retentionService);
  }
}
