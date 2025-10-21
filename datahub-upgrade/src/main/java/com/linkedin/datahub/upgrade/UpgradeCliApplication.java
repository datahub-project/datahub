package com.linkedin.datahub.upgrade;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@Configuration
@Import(UpgradeConfigurationSelector.class)
public class UpgradeCliApplication {
  public static void main(String[] args) {
    new SpringApplicationBuilder(UpgradeCliApplication.class, UpgradeCli.class)
        .web(WebApplicationType.NONE)
        .run(args);
  }
}
