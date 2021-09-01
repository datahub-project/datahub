package com.linkedin.datahub.upgrade;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;


@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {RestClientAutoConfiguration.class}, scanBasePackages = {
    "com.linkedin.gms.factory.common", "com.linkedin.gms.factory.kafka", "com.linkedin.gms.factory.search",
    "com.linkedin.datahub.upgrade.config", "com.linkedin.gms.factory.entity"})
public class UpgradeCliApplication {
  public static void main(String[] args) {
    new SpringApplicationBuilder(UpgradeCliApplication.class, UpgradeCli.class).web(WebApplicationType.NONE).run(args);
  }
}
