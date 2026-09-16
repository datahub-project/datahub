package com.linkedin.metadata.restli;

import com.datahub.auth.authentication.filter.AuthenticationEnforcementFilter;
import com.datahub.auth.authentication.filter.AuthenticationExtractionFilter;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.r2.transport.http.server.RAPJakartaServlet;
import com.linkedin.restli.server.RestliHandlerServlet;
import java.util.Collections;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
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
  public FilterRegistrationBean<AuthenticationEnforcementFilter>
      authenticationEnforcementFilterRegistrationBean(
          @Qualifier("restliServletRegistration")
              ServletRegistrationBean<RestliHandlerServlet> servlet,
          AuthenticationEnforcementFilter authenticationEnforcementFilter) {
    FilterRegistrationBean<AuthenticationEnforcementFilter> registrationBean =
        new FilterRegistrationBean<>();
    registrationBean.setServletRegistrationBeans(Collections.singletonList(servlet));
    registrationBean.setUrlPatterns(Collections.singletonList("/gms/*"));
    registrationBean.setServletNames(Collections.singletonList(servlet.getServletName()));
    registrationBean.setOrder(1);
    registrationBean.setFilter(authenticationEnforcementFilter);
    return registrationBean;
  }

  @Bean
  public AuthenticationEnforcementFilter authenticationEnforcementFilter() {
    return new AuthenticationEnforcementFilter();
  }

  @Bean
  public AuthenticationExtractionFilter authenticationExtractionFilter() {
    return new AuthenticationExtractionFilter();
  }

  @Bean
  public FilterRegistrationBean<AuthenticationExtractionFilter>
      authenticationExtractionFilterRegistrationBean(
          @Qualifier("restliServletRegistration")
              ServletRegistrationBean<RestliHandlerServlet> servlet,
          AuthenticationExtractionFilter authenticationExtractionFilter) {
    FilterRegistrationBean<AuthenticationExtractionFilter> registrationBean =
        new FilterRegistrationBean<>();
    registrationBean.setServletRegistrationBeans(Collections.singletonList(servlet));
    registrationBean.setUrlPatterns(Collections.singletonList("/gms/*"));
    registrationBean.setServletNames(Collections.singletonList(servlet.getServletName()));
    registrationBean.setOrder(0); // Run before AuthenticationEnforcementFilter (order 1)
    registrationBean.setFilter(authenticationExtractionFilter);
    return registrationBean;
  }

  @Bean
  @ConditionalOnBean
  public ElasticSearchConfiguration elasticSearchConfiguration(
      final ConfigurationProvider configurationProvider) {
    return configurationProvider.getElasticSearch();
  }
}
