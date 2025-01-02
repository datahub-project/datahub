package com.linkedin.gms.servlet;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan(
    basePackages = {
      "io.datahubproject.openapi.config",
      "io.datahubproject.openapi.health",
      "io.datahubproject.openapi.openlineage",
      "io.datahubproject.openapi.operations",
      "io.datahubproject.openapi.platform",
      "io.datahubproject.openapi.relationships",
      "io.datahubproject.openapi.timeline",
      "io.datahubproject.openapi.entities",
      "io.datahubproject.openapi.v1",
      "io.datahubproject.openapi.v2",
      "io.datahubproject.openapi.v3",
      "com.linkedin.gms.factory.timeline",
      "org.springdoc"
    })
@Configuration
public class OpenAPIServletConfig {}
