package com.linkedin.gms.servlet;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan(
    basePackages = {
      "io.datahubproject.openapi.metadatatests",
      "io.datahubproject.openapi.scim",
      "io.datahubproject.openapi.events",
      "org.apache.directory.scim.server",
      "com.linkedin.gms.factory.scim",
      "com.linkedin.gms.factory.event",
      "com.linkedin.gms.factory.kafka",
    })
@Configuration
public class AcrylOpenAPIServletConfig {}
