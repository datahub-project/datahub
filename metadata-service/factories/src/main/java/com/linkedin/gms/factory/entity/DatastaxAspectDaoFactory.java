package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.datastax.DatastaxAspectDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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

  @Value("${DAO_SERVICE_LAYER:ebean}")
  private String daoServiceLayer;

  @Bean(name = "datastaxAspectDao")
  @DependsOn({"gmsDatastaxServiceConfig"})
  @Nonnull
  protected DatastaxAspectDao createInstance() {
    return daoServiceLayer == "datastax"
            ? new DatastaxAspectDao((Map<String, String>)applicationContext.getBean("gmsDatastaxServiceConfig"))
            : null;
  }
}