package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "homePage" configuration block in application.yaml.on.yml */
@Data
public class HomePageConfiguration {
  /** First section to show in the personal sidebar on the home page */
  public String firstInPersonalSidebar;
}
