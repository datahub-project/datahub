package com.linkedin.gms;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

/**
 * Common configuration for all servlets. Generally this list also includes dependencies of the
 * embedded MAE/MCE consumers.
 */
@ComponentScan(
    basePackages = {
      "com.linkedin.metadata.boot",
      "com.linkedin.metadata.service",
      "com.datahub.event",
      "com.linkedin.gms.factory.config",
      "com.linkedin.gms.factory.entityregistry",
      "com.linkedin.gms.factory.common",
      "com.linkedin.gms.factory.entity",
      "com.linkedin.gms.factory.kafka",
      "com.linkedin.gms.factory.kafka.common",
      "com.linkedin.gms.factory.kafka.schemaregistry",
      "com.linkedin.metadata.boot.kafka",
      "com.linkedin.metadata.kafka",
      "com.linkedin.metadata.dao.producer",
      "com.linkedin.gms.factory.entity.update.indices",
      "com.linkedin.gms.factory.entityclient",
      "com.linkedin.gms.factory.form",
      "com.linkedin.gms.factory.incident",
      "com.linkedin.gms.factory.timeline.eventgenerator",
      "io.datahubproject.metadata.jobs.common.health.kafka",
      "com.linkedin.gms.factory.context",
      "com.linkedin.gms.factory.auth",
      "com.linkedin.gms.factory.search",
      "com.linkedin.gms.factory.secret",
      "com.linkedin.gms.factory.timeseries",
      "com.linkedin.gms.factory.plugins",
      "com.linkedin.gms.factory.change",
      "com.datahub.event.hook",
      "com.linkedin.gms.factory.notifications",
      "com.linkedin.gms.factory.telemetry"
    })
@Configuration
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
public class CommonApplicationConfig {

  @Autowired private Environment environment;

  @Bean
  public WebServerFactoryCustomizer<JettyServletWebServerFactory> jettyCustomizer() {
    return factory -> {
      // Configure HTTP
      factory.addServerCustomizers(
          server -> {
            // HTTP Configuration
            HttpConfiguration httpConfig = new HttpConfiguration();
            httpConfig.setRequestHeaderSize(32768);

            // HTTP Connector
            ServerConnector connector =
                new ServerConnector(server, new HttpConnectionFactory(httpConfig));

            // Get port from environment directly
            int port = environment.getProperty("server.port", Integer.class, 8080);
            connector.setPort(port);

            // Set connectors
            server.setConnectors(new Connector[] {connector});

            // JMX Configuration
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
            mBeanContainer.beanAdded(null, LoggerFactory.getILoggerFactory());
            server.addBean(mBeanContainer);
          });
    };
  }
}
