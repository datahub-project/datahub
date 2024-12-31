package com.linkedin.gms;

import static com.linkedin.metadata.boot.OnBootApplicationListener.SCHEMA_REGISTRY_SERVLET_NAME;

import com.datahub.auth.authentication.filter.AuthenticationFilter;
import com.datahub.gms.servlet.Config;
import com.datahub.gms.servlet.ConfigSearchExport;
import com.datahub.gms.servlet.HealthCheck;
import com.linkedin.gms.servlet.AuthServletConfig;
import com.linkedin.gms.servlet.GraphQLServletConfig;
import com.linkedin.gms.servlet.OpenAPIServletConfig;
import com.linkedin.gms.servlet.RestliServletConfig;
import com.linkedin.gms.servlet.SchemaRegistryServletConfig;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.FilterRegistration;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletRegistration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.context.support.HttpRequestHandlerServlet;
import org.springframework.web.servlet.DispatcherServlet;

/** This class is before Spring Context is loaded, previously web.xml based */
public class WebApplicationInitializer
    implements org.springframework.web.WebApplicationInitializer {
  @Override
  public void onStartup(ServletContext container) {
    AnnotationConfigWebApplicationContext rootContext = new AnnotationConfigWebApplicationContext();
    rootContext.register(CommonApplicationConfig.class);

    ContextLoaderListener contextLoaderListener = new ContextLoaderListener(rootContext);
    container.addListener(contextLoaderListener);
    container.setInitParameter(
        "contextInitializerClasses", "com.linkedin.gms.SpringApplicationInitializer");

    // Auth filter
    List<String> servletNames = new ArrayList<>();

    // Independent dispatcher
    schemaRegistryServlet(container);

    // Spring Dispatcher servlets
    DispatcherServlet dispatcherServlet = new DispatcherServlet(rootContext);
    servletNames.add(authServlet(rootContext, dispatcherServlet, container));
    servletNames.add(graphQLServlet(rootContext, dispatcherServlet, container));
    servletNames.add(openAPIServlet(rootContext, dispatcherServlet, container));
    // Restli non-Dispatcher default
    servletNames.add(restliServlet(rootContext, container));

    FilterRegistration.Dynamic filterRegistration =
        container.addFilter("authenticationFilter", AuthenticationFilter.class);
    filterRegistration.setAsyncSupported(true);
    filterRegistration.addMappingForServletNames(
        EnumSet.of(DispatcherType.ASYNC, DispatcherType.REQUEST),
        false,
        servletNames.toArray(String[]::new));

    // Non-Spring servlets
    healthCheckServlet(container);
    configServlet(container);
  }

  /*
   * This is a servlet exclusive to DataHub's implementation of the Confluent OpenAPI spec which is not behind
   * DataHub's authentication layer as it is not compatible with confluent consumers & producers.
   */
  private void schemaRegistryServlet(ServletContext container) {
    AnnotationConfigWebApplicationContext webContext = new AnnotationConfigWebApplicationContext();
    webContext.setId(SCHEMA_REGISTRY_SERVLET_NAME);
    webContext.register(SchemaRegistryServletConfig.class);

    DispatcherServlet dispatcherServlet = new DispatcherServlet(webContext);
    ServletRegistration.Dynamic registration =
        container.addServlet(SCHEMA_REGISTRY_SERVLET_NAME, dispatcherServlet);
    registration.addMapping("/schema-registry/*");
    registration.setLoadOnStartup(1);
    registration.setAsyncSupported(true);
  }

  private String authServlet(
      AnnotationConfigWebApplicationContext rootContext,
      DispatcherServlet dispatcherServlet,
      ServletContext container) {
    final String servletName = "dispatcher-auth";
    rootContext.register(AuthServletConfig.class);

    ServletRegistration.Dynamic registration = container.addServlet(servletName, dispatcherServlet);
    registration.addMapping("/auth/*");
    registration.setLoadOnStartup(5);
    registration.setAsyncSupported(true);

    return servletName;
  }

  private String graphQLServlet(
      AnnotationConfigWebApplicationContext rootContext,
      DispatcherServlet dispatcherServlet,
      ServletContext container) {
    final String servletName = "dispatcher-graphql";
    rootContext.register(GraphQLServletConfig.class);

    ServletRegistration.Dynamic registration = container.addServlet(servletName, dispatcherServlet);
    registration.addMapping("/api/*");
    registration.setLoadOnStartup(5);
    registration.setAsyncSupported(true);

    return servletName;
  }

  private String openAPIServlet(
      AnnotationConfigWebApplicationContext rootContext,
      DispatcherServlet dispatcherServlet,
      ServletContext container) {
    final String servletName = "dispatcher-openapi";
    rootContext.register(OpenAPIServletConfig.class);

    ServletRegistration.Dynamic registration = container.addServlet(servletName, dispatcherServlet);
    registration.addMapping("/openapi/*");
    registration.setLoadOnStartup(5);
    registration.setAsyncSupported(true);

    return servletName;
  }

  private String restliServlet(
      AnnotationConfigWebApplicationContext rootContext, ServletContext container) {
    final String servletName = "restliRequestHandler";

    rootContext.register(RestliServletConfig.class);

    ServletRegistration.Dynamic registration =
        container.addServlet(servletName, HttpRequestHandlerServlet.class);
    registration.addMapping("/*");
    registration.setLoadOnStartup(10);
    registration.setAsyncSupported(true);
    registration.setInitParameter(
        "org.springframework.web.servlet.FrameworkServlet.ORDER",
        String.valueOf(Integer.MAX_VALUE - 1));

    return servletName;
  }

  private void healthCheckServlet(ServletContext container) {
    ServletRegistration.Dynamic registration =
        container.addServlet("healthCheck", new HealthCheck());
    registration.addMapping("/health");
    registration.setLoadOnStartup(15);
    registration.setAsyncSupported(true);
  }

  private void configServlet(ServletContext container) {
    ServletRegistration.Dynamic registration = container.addServlet("config", new Config());
    registration.addMapping("/config");
    registration.setLoadOnStartup(15);
    registration.setAsyncSupported(true);

    ServletRegistration.Dynamic registration2 =
        container.addServlet("config-search-export", new ConfigSearchExport());
    registration2.addMapping("/config/search/export");
    registration2.setLoadOnStartup(15);
    registration2.setAsyncSupported(true);
  }
}
