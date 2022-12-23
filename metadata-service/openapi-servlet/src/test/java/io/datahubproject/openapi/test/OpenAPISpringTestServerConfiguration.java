package io.datahubproject.openapi.test;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.servlet.DispatcherServlet;


@TestConfiguration
@ComponentScan(basePackages = {"io.datahubproject.openapi.schema.registry", "com.linkedin.metadata.schema.registry",
    "io.datahubproject.openapi.schema.registry"})
public class OpenAPISpringTestServerConfiguration {

  @LocalServerPort
  private int port;

  @Bean(name = "testRestTemplate")
  public TestRestTemplate testRestTemplate() {
    var restTemplate = new RestTemplateBuilder().rootUri("http://localhost:" + port);
    return new TestRestTemplate(restTemplate);
  }

  @Bean
  public DispatcherServlet dispatcherServlet() {
    return new DispatcherServlet();
  }

  @Bean
  public ServletRegistrationBean<DispatcherServlet> servletRegistrationBean(DispatcherServlet dispatcherServlet) {
    return new ServletRegistrationBean<>(dispatcherServlet, "/");
  }
}
