package com.linkedin.gms;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;

import com.datahub.auth.authentication.filter.AuthenticationFilter;
import com.datahub.gms.servlet.Config;
import com.datahub.gms.servlet.ConfigSearchExport;
import com.datahub.gms.servlet.HealthCheck;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.linkedin.r2.transport.http.server.RAPJakartaServlet;
import com.linkedin.restli.server.RestliHandlerServlet;
import io.datahubproject.iceberg.catalog.rest.common.IcebergJsonConverter;
import io.datahubproject.openapi.converter.StringToChangeCategoryConverter;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.rest.RESTSerializers;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Common configuration for all servlets. Generally this list also includes dependencies of the
 * embedded MAE/MCE consumers.
 */
@Slf4j
@Configuration
@Order(1)
@EnableWebMvc
@ComponentScan(
    basePackages = {"io.datahubproject.openapi.schema.registry.config", "com.linkedin.gms.servlet"})
public class ServletConfig implements WebMvcConfigurer {

  @Value("${datahub.gms.async.request-timeout-ms}")
  private long asyncTimeoutMilliseconds;

  @Bean
  public FilterRegistrationBean<AuthenticationFilter> authFilter(AuthenticationFilter filter) {
    FilterRegistrationBean<AuthenticationFilter> registration = new FilterRegistrationBean<>();
    registration.setFilter(filter);
    registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
    registration.setAsyncSupported(true);

    // Register filter for all paths - exclusions are handled by shouldNotFilter()
    registration.addUrlPatterns("/*");

    return registration;
  }

  @Bean
  public ServletRegistrationBean<HealthCheck> healthCheckServlet() {
    ServletRegistrationBean<HealthCheck> registration =
        new ServletRegistrationBean<>(new HealthCheck());
    registration.setName("healthCheck");
    registration.addUrlMappings("/health");
    registration.setLoadOnStartup(15);
    registration.setAsyncSupported(true);
    return registration;
  }

  @Bean
  public ServletRegistrationBean<Config> configServlet() {
    ServletRegistrationBean<Config> registration = new ServletRegistrationBean<>(new Config());
    registration.setName("config");
    registration.addUrlMappings("/config");
    registration.setLoadOnStartup(15);
    registration.setAsyncSupported(true);
    return registration;
  }

  @Bean
  public ServletRegistrationBean<ConfigSearchExport> configSearchExportServlet() {
    ServletRegistrationBean<ConfigSearchExport> registration =
        new ServletRegistrationBean<>(new ConfigSearchExport());
    registration.setName("config-search-export");
    registration.addUrlMappings("/config/search/export");
    registration.setLoadOnStartup(15);
    registration.setAsyncSupported(true);
    return registration;
  }

  /**
   * SpringBoot is now the default, explicitly map these to legacy rest.li servlet. Additions are
   * more likely to be built on the Spring side so we're preventing unexpected behavior for the most
   * likely changes. Rest.li API is intended to be deprecated and removed.
   *
   * @param r2Servlet the restli servlet
   * @return registration
   */
  @Bean
  public ServletRegistrationBean<RestliHandlerServlet> restliServletRegistration(
      RAPJakartaServlet r2Servlet) {
    ServletRegistrationBean<RestliHandlerServlet> registration =
        new ServletRegistrationBean<>(new RestliHandlerServlet(r2Servlet));
    registration.addUrlMappings(
        "/aspects/*",
        "/entities/*",
        "/entitiesV2/*",
        "/entitiesVersionedV2/*",
        "/usageStats/*",
        "/platform/*",
        "/relationships/*",
        "/analytics/*",
        "/operations/*",
        "/runs/*");
    registration.setLoadOnStartup(2);
    registration.setOrder(Integer.MAX_VALUE); // lowest priority
    return registration;
  }

  @Override
  public void configureMessageConverters(List<HttpMessageConverter<?>> messageConverters) {
    messageConverters.add(new StringHttpMessageConverter());
    messageConverters.add(new ByteArrayHttpMessageConverter());
    messageConverters.add(new FormHttpMessageConverter());
    messageConverters.add(createIcebergMessageConverter());

    ObjectMapper objectMapper = new ObjectMapper();
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    MappingJackson2HttpMessageConverter jsonConverter =
        new MappingJackson2HttpMessageConverter(objectMapper);
    messageConverters.add(jsonConverter);
  }

  private HttpMessageConverter<?> createIcebergMessageConverter() {
    ObjectMapper objectMapper = new ObjectMapper();
    MappingJackson2HttpMessageConverter jsonConverter = new IcebergJsonConverter(objectMapper);

    objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    objectMapper.setPropertyNamingStrategy(new PropertyNamingStrategies.KebabCaseStrategy());
    RESTSerializers.registerAll(objectMapper);
    return jsonConverter;
  }

  @Override
  public void addFormatters(FormatterRegistry registry) {
    registry.addConverter(new StringToChangeCategoryConverter());
  }

  @Override
  public void configureAsyncSupport(@Nonnull AsyncSupportConfigurer configurer) {
    WebMvcConfigurer.super.configureAsyncSupport(configurer);
    configurer.setDefaultTimeout(asyncTimeoutMilliseconds);
  }
}
