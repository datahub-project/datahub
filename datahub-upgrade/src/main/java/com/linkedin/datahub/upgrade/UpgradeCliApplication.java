package com.linkedin.datahub.upgrade;

import com.linkedin.gms.factory.telemetry.ScheduledAnalyticsFactory;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;


@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class})
@ComponentScan(basePackages = {"com.linkedin.gms.factory", "com.linkedin.datahub.upgrade.config"},
    excludeFilters = {
    @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = ScheduledAnalyticsFactory.class)})
public class UpgradeCliApplication {
  public static void main(String[] args) {
    new SpringApplicationBuilder(UpgradeCliApplication.class, UpgradeCli.class).web(WebApplicationType.NONE).run(args);
  }
}
