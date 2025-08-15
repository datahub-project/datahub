package com.linkedin.gms.servlet;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@ComponentScan(
    basePackages = {
      "com.datahub.graphql",
      "com.linkedin.gms.factory.graphql",
      "com.linkedin.gms.factory.timeline",
      "com.linkedin.gms.factory.usage",
      "com.linkedin.gms.factory.recommendation",
      "com.linkedin.gms.factory.ownership",
      "com.linkedin.gms.factory.settings",
      "com.linkedin.gms.factory.lineage",
      "com.linkedin.gms.factory.query",
      "com.linkedin.gms.factory.ermodelrelation",
      "com.linkedin.gms.factory.dataproduct",
      "com.linkedin.gms.factory.businessattribute",
      "com.linkedin.gms.factory.connection"
    })
@Configuration
public class GraphQLServletConfig {}
