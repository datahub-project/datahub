package com.linkedin.datahub.upgrade;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;


@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class}, scanBasePackages = {
    "com.linkedin.gms.factory", "com.linkedin.datahub.upgrade.config"})
public class UpgradeCliApplication {
  public static void main(String[] args) {
    new SpringApplicationBuilder(UpgradeCliApplication.class, UpgradeCli.class).web(WebApplicationType.NONE).run(args);
  }
}
