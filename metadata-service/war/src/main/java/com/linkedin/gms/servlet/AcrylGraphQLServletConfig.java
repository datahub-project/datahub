package com.linkedin.gms.servlet;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan(
    basePackages = {
      "com.linkedin.gms.factory.test",
      "com.linkedin.gms.factory.assertions",
      "com.linkedin.gms.factory.monitor",
      "com.linkedin.gms.factory.integration",
      "com.linkedin.gms.factory.connection",
      "com.linkedin.gms.factory.subscription",
      "com.linkedin.gms.factory.share",
      "com.linkedin.gms.factory.user",
      "com.linkedin.gms.factory.actionrequest",
      "com.linkedin.gms.factory.ownership",
      "com.linkedin.gms.factory.domain",
      "com.linkedin.gms.factory.dataproduct"
    })
@Configuration
public class AcrylGraphQLServletConfig {}
