package com.linkedin.gms.factory.dataplatform;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.metadata.aspect.DataPlatformAspect;
import com.linkedin.metadata.dao.ImmutableLocalDAO;
import com.linkedin.metadata.resources.dataplatform.utils.DataPlatformsUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DataPlatformLocalDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "dataPlatformLocalDAO")
  protected ImmutableLocalDAO createInstance() {
    return new ImmutableLocalDAO<>(DataPlatformAspect.class, DataPlatformsUtil.getDataPlatformInfoMap(),
        DataPlatformUrn.class);
  }
}
