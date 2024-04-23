package com.linkedin.gms.servlet;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan(
    basePackages = {
      "io.datahubproject.openapi.metadatatests",
      "io.datahubproject.openapi.scim.repositories",
      "org.apache.directory.scim.server"
    })
@Configuration
public class AcrylOpenAPIServletConfig {}
