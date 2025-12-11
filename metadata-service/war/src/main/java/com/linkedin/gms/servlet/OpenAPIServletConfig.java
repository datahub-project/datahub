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
      "io.datahubproject.openapi.analytics",
      "com.linkedin.gms.factory.timeline",
      "com.linkedin.gms.factory.event",
      "com.linkedin.gms.factory.s3",
      "org.springdoc",
      "com.linkedin.gms.factory.system_info"
    })
@Configuration
public class OpenAPIServletConfig {}
