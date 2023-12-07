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
