package com.linkedin.datahub.upgrade.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Component;

@Component
public class ConditionChecker {
  @Autowired private ApplicationArguments applicationArguments;

  public boolean isDryRun() {
    return applicationArguments.getOptionNames().contains("dryRun");
  }
}
