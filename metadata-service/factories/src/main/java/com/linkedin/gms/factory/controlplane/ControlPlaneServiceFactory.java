package com.linkedin.gms.factory.controlplane;

import com.linkedin.metadata.config.ControlPlaneConfiguration;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.service.ControlPlaneService;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ControlPlaneServiceFactory {

  @Autowired private DataHubAppConfiguration appConfig;

  @Bean(name = "controlPlaneService")
  @Nonnull
  protected ControlPlaneService getInstance() {
    ControlPlaneConfiguration config = appConfig.getControlPlane();
    if (config == null) {
      config = new ControlPlaneConfiguration();
    }
    return new ControlPlaneService(config);
  }
}
