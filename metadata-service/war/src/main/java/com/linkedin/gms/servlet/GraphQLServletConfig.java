/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
