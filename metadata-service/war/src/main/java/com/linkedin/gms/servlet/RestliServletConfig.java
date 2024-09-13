package com.linkedin.gms.servlet;

import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import com.linkedin.r2.transport.http.server.RAPJakartaServlet;
import com.linkedin.restli.server.RestliHandlerServlet;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.web.HttpRequestHandler;

@ComponentScan(basePackages = {"com.linkedin.restli.server"})
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
@Configuration
public class RestliServletConfig {

  // ensure ordering by depending on required entityAspectDao
  @Bean("restliRequestHandler")
  public HttpRequestHandler restliHandlerServlet(
      final RAPJakartaServlet r2Servlet, @Qualifier("entityAspectDao") final AspectDao aspectDao) {
    return new RestliHandlerServlet(r2Servlet);
  }
}
