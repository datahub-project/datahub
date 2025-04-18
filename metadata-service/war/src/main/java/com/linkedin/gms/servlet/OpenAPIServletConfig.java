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
      "com.linkedin.gms.factory.event",
<<<<<<< HEAD
      "org.springdoc",
      "io.datahubproject.openapi.tests"
=======
      "org.springdoc"
>>>>>>> dbad52283b070c7cc136306c1553770db2f72105
    })
@Configuration
public class OpenAPIServletConfig {}
