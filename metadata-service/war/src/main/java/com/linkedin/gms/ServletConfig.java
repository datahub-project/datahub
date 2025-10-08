package com.linkedin.gms;

import static com.linkedin.metadata.Constants.INGESTION_MAX_SERIALIZED_STRING_LENGTH;
import static com.linkedin.metadata.Constants.MAX_JACKSON_STRING_SIZE;
import static io.acryl.admin.grafana.GrafanaConfiguration.GRAFANA_SERVLET_NAME;

import com.datahub.auth.authentication.filter.AuthenticationEnforcementFilter;
import com.datahub.auth.authentication.filter.AuthenticationExtractionFilter;
import com.datahub.gms.servlet.Config;
import com.datahub.gms.servlet.ConfigSearchExport;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.linkedin.gms.factory.scim.ScimpleSpringConfiguration;
import com.linkedin.metadata.config.GMSConfiguration;
import com.linkedin.metadata.utils.BasePathUtils;
import com.linkedin.r2.transport.http.server.RAPJakartaServlet;
import com.linkedin.restli.server.RestliHandlerServlet;
import io.acryl.admin.grafana.GrafanaServlet;
import io.datahubproject.iceberg.catalog.rest.common.IcebergJsonConverter;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.converter.StringToChangeCategoryConverter;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.directory.scim.core.json.ObjectMapperFactory;
import org.apache.directory.scim.core.schema.SchemaRegistry;
import org.apache.directory.scim.protocol.Constants;
import org.apache.iceberg.rest.RESTSerializers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.format.FormatterRegistry;
import org.springframework.http.MediaType;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.xml.MappingJackson2XmlHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Common configuration for all servlets. Generally this list also includes dependencies of the
 * embedded MAE/MCE consumers.
 */
@Configuration
@Order(1)
@EnableWebMvc
@ComponentScan(
    basePackages = {"io.datahubproject.openapi.schema.registry.config", "com.linkedin.gms.servlet"})
@Slf4j
public class ServletConfig implements WebMvcConfigurer {
  @Autowired private TracingInterceptor tracingInterceptor;

  @Autowired private SchemaRegistry schemaRegistry;

  @Value("${datahub.gms.async.request-timeout-ms}")
  private long asyncTimeoutMilliseconds;

  @Autowired private GMSConfiguration gmsConfiguration;

  @Bean
  public FilterRegistrationBean<AuthenticationExtractionFilter> authExtractionFilter(
      AuthenticationExtractionFilter filter) {
    FilterRegistrationBean<AuthenticationExtractionFilter> registration =
        new FilterRegistrationBean<>();
    registration.setFilter(filter);
    registration.setOrder(Ordered.HIGHEST_PRECEDENCE); // Run FIRST to extract authentication info
    registration.setAsyncSupported(true);

    // Register for all paths - this filter ALWAYS runs to extract auth info
    registration.addUrlPatterns("/*");

    return registration;
  }

  @Bean
  public FilterRegistrationBean<AuthenticationEnforcementFilter> authFilter(
      AuthenticationEnforcementFilter filter) {
    FilterRegistrationBean<AuthenticationEnforcementFilter> registration =
        new FilterRegistrationBean<>();
    registration.setFilter(filter);
    registration.setOrder(
        Ordered.HIGHEST_PRECEDENCE + 1); // Run SECOND after AuthenticationExtractionFilter
    registration.setAsyncSupported(true);

    // Register filter for all paths - exclusions are handled by shouldNotFilter()
    registration.addUrlPatterns("/*");

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

  @Bean
  public ServletRegistrationBean<GrafanaServlet> grafanaDashboardServlet(
      final GrafanaServlet grafanaServlet) {
    ServletRegistrationBean<GrafanaServlet> registration =
        new ServletRegistrationBean<>(grafanaServlet);
    registration.setName(GRAFANA_SERVLET_NAME);
    registration.addUrlMappings("/admin/dashboard/*");
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

    // Spring Boot automatically handles context path prefixing for servlet registrations
    // So we use relative paths here, and Spring Boot will add the context path automatically
    String[] urlMappings = {
      "/aspects/*",
      "/entities/*",
      "/entitiesV2/*",
      "/entitiesVersionedV2/*",
      "/usageStats/*",
      "/platform/*",
      "/relationships/*",
      "/analytics/*",
      "/operations/*",
      "/runs/*",
      "/test/*"
    };

    log.info(
        "Registering RestLi servlet with gmsBasePath='{}', urlMappings={} (Spring Boot will add context path automatically)",
        BasePathUtils.resolveBasePath(
            gmsConfiguration.getBasePathEnabled(), gmsConfiguration.getBasePath()),
        Arrays.toString(urlMappings));

    registration.addUrlMappings(urlMappings);
    registration.setLoadOnStartup(2);
    registration.setOrder(Integer.MAX_VALUE); // lowest priority
    return registration;
  }

  @Override
  public void configurePathMatch(PathMatchConfigurer configurer) {
    configurer.addPathPrefix(
        "/openapi",
        c -> c.getPackage().getName().startsWith(ScimpleSpringConfiguration.SCIM_IMPL_PACKAGE));
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
    objectMapper.registerModule(new Jdk8Module());
    MappingJackson2HttpMessageConverter jsonConverter =
        new MappingJackson2HttpMessageConverter(objectMapper);
    messageConverters.add(jsonConverter);

    // Saas Only
    configureMessageConvertersSaas(messageConverters);
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

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(tracingInterceptor).addPathPatterns("/**");
  }

  private void configureMessageConvertersSaas(List<HttpMessageConverter<?>> messageConverters) {
    // remove OSS
    messageConverters.remove(messageConverters.size() - 1);
    // Add Saas
    messageConverters.add(jsonMessageConverter(schemaRegistry));
    messageConverters.add(xmlMessageConverter(schemaRegistry));
  }

  private MappingJackson2HttpMessageConverter jsonMessageConverter(SchemaRegistry schemaRegistry) {
    ObjectMapper objectMapper = ObjectMapperFactory.createObjectMapper(schemaRegistry);
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    MappingJackson2HttpMessageConverter converter =
        new MappingJackson2HttpMessageConverter(objectMapper);
    converter.setSupportedMediaTypes(
        Arrays.asList(
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_XML,
            MediaType.valueOf(Constants.SCIM_CONTENT_TYPE),
            MediaType.valueOf("application/json-patch+json"),
            MediaType.valueOf("application/vnd.schemaregistry.v1+json")));
    return converter;
  }

  private MappingJackson2XmlHttpMessageConverter xmlMessageConverter(
      SchemaRegistry schemaRegistry) {
    ObjectMapper objectMapper = ObjectMapperFactory.createXmlObjectMapper(schemaRegistry);
    MappingJackson2XmlHttpMessageConverter converter =
        new MappingJackson2XmlHttpMessageConverter(objectMapper);
    converter.setSupportedMediaTypes(
        Arrays.asList(
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_XML,
            MediaType.valueOf(Constants.SCIM_CONTENT_TYPE)));
    return converter;
  }

  @Override
  public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
    converters.add(jsonMessageConverter(schemaRegistry));
    converters.add(xmlMessageConverter(schemaRegistry));
  }
}
