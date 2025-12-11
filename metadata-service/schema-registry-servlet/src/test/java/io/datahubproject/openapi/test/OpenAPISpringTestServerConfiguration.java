/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.test;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.servlet.DispatcherServlet;

@TestConfiguration
@ComponentScan(
    basePackages = {
      "io.datahubproject.openapi.schema.registry",
      "com.linkedin.metadata.schema.registry"
    })
public class OpenAPISpringTestServerConfiguration {

  @Bean
  public DispatcherServlet dispatcherServlet() {
    return new DispatcherServlet();
  }

  @Bean
  public ServletRegistrationBean<DispatcherServlet> servletRegistrationBean(
      DispatcherServlet dispatcherServlet) {
    return new ServletRegistrationBean<>(dispatcherServlet, "/");
  }
}
