package com.linkedin.gms.servlet;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@EnableWebMvc
@ComponentScan(basePackages = {"io.datahubproject.iceberg.catalog.rest"})
@Configuration
public class IcebergCatalogServletConfig {}
