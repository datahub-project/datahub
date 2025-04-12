package com.linkedin.gms.servlet;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan(
    basePackages = {
      "io.datahubproject.openapi.metadatatests",
      "io.datahubproject.openapi.scim",
      "org.apache.directory.scim.server",
      "com.linkedin.gms.factory.scim"
    })
@Configuration
public class AcrylOpenAPIServletConfig {}
