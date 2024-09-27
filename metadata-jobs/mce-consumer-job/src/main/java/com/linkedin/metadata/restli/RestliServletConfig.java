package com.linkedin.metadata.restli;

import com.datahub.auth.authentication.filter.AuthenticationFilter;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.r2.transport.http.server.RAPJakartaServlet;
import com.linkedin.restli.server.RestliHandlerServlet;
import java.util.Collections;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({SystemAuthenticationFactory.class})
public class RestliServletConfig {

  @Value("${server.port}")
  private int configuredPort;

  @Value("${entityClient.retryInterval:2}")
  private int retryInterval;

  @Value("${entityClient.numRetries:3}")
  private int numRetries;

  @Bean("restliServletRegistration")
  public ServletRegistrationBean<RestliHandlerServlet> restliServletRegistration(
      RestliHandlerServlet servlet) {

    ServletRegistrationBean<RestliHandlerServlet> registrationBean =
        new ServletRegistrationBean<>(servlet, "/gms/*");
    registrationBean.setName("restliHandlerServlet");
    return registrationBean;
  }

  @Bean("restliHandlerServlet")
  public RestliHandlerServlet restliHandlerServlet(final RAPJakartaServlet r2Servlet) {
    return new RestliHandlerServlet(r2Servlet);
  }

  @Bean
  public FilterRegistrationBean<AuthenticationFilter> authenticationFilterRegistrationBean(
      @Qualifier("restliServletRegistration") ServletRegistrationBean<RestliHandlerServlet> servlet,
      AuthenticationFilter authenticationFilter) {
    FilterRegistrationBean<AuthenticationFilter> registrationBean = new FilterRegistrationBean<>();
    registrationBean.setServletRegistrationBeans(Collections.singletonList(servlet));
    registrationBean.setUrlPatterns(Collections.singletonList("/gms/*"));
    registrationBean.setServletNames(Collections.singletonList(servlet.getServletName()));
    registrationBean.setOrder(1);
    registrationBean.setFilter(authenticationFilter);
    return registrationBean;
  }

  @Bean
  public AuthenticationFilter authenticationFilter() {
    return new AuthenticationFilter();
  }
}
