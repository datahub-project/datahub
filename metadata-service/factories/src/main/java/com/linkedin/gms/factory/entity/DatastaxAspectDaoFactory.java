package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.datastax.DatastaxAspectDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import java.util.Map;

import javax.annotation.Nonnull;


@Configuration
public class DatastaxAspectDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "datastaxAspectDao")
  @ConditionalOnProperty(name = "ENTITY_SERVICE_IMPL", havingValue = "datastax")
  @DependsOn({"gmsDatastaxServiceConfig"})
  @Nonnull
  protected DatastaxAspectDao createInstance() {
    return new DatastaxAspectDao((Map<String, String>) applicationContext.getBean("gmsDatastaxServiceConfig"));
  }
}