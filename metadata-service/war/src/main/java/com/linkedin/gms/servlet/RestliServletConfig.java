package com.linkedin.gms.servlet;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.r2.transport.http.server.RAPJakartaServlet;
import com.linkedin.restli.server.RestliHandlerServlet;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.context.support.HttpRequestHandlerServlet;

@ComponentScan(basePackages = {"com.linkedin.restli.server"})
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
@Configuration
public class RestliServletConfig {
  @Bean("restliRequestHandler")
  public HttpRequestHandlerServlet restliHandlerServlet(final RAPJakartaServlet r2Servlet) {
    return new RestliHandlerServlet(r2Servlet);
  }
}
