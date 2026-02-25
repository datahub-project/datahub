package com.linkedin.gms;

import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanServer;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.ee10.servlet.ServletHandler;
import org.eclipse.jetty.http.UriCompliance;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
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
      "com.linkedin.gms.factory.telemetry",
      "com.linkedin.gms.factory.trace",
      "com.linkedin.gms.factory.kafka.trace",
      "com.linkedin.gms.factory.system_info",
      "com.linkedin.gms.factory.consistency",
      "com.linkedin.metadata.aspect.consistency.check",
      "com.linkedin.metadata.aspect.consistency.fix",
    })
@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
public class CommonApplicationConfig {

  @Autowired private Environment environment;

  @Bean
  @ConditionalOnWebApplication
  public WebServerFactoryCustomizer<JettyServletWebServerFactory> jettyCustomizer() {
    return factory -> {
      factory.addServerCustomizers(
          server -> {

            // --- HTTP Configuration (always created) ---
            HttpConfiguration httpConfig = new HttpConfiguration();
            httpConfig.setRequestHeaderSize(32768);

            // Security: Disable server version disclosure
            httpConfig.setSendServerVersion(false);
            httpConfig.setSendDateHeader(false);

            // See https://github.com/jetty/jetty.project/issues/11890
            // Configure URI compliance to allow encoded slashes
            httpConfig.setUriCompliance(
                UriCompliance.from(
                    Set.of(
                        UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR,
                        UriCompliance.Violation.AMBIGUOUS_PATH_ENCODING,
                        UriCompliance.Violation.SUSPICIOUS_PATH_CHARACTERS)));
            // set this for Servlet 6+
            server
                .getContainedBeans(ServletHandler.class)
                .forEach(handler -> handler.setDecodeAmbiguousURIs(true));

            // --- Ports ---
            int httpPort = environment.getProperty("server.port", Integer.class, 8080);
            int httpsPort = environment.getProperty("server.ssl.port", Integer.class, 8443);

            // --- SSL Config ---
            String keyStorePath = environment.getProperty("server.ssl.key-store");
            String keyStorePassword = environment.getProperty("server.ssl.key-store-password");
            String keyStoreType = environment.getProperty("server.ssl.key-store-type", "PKCS12");
            String keyAlias = environment.getProperty("server.ssl.key-alias");

            ServerConnector connector;

            // --- SSL Only if Configured ---
            if (keyStorePath != null
                && !keyStorePath.isBlank()
                && keyStorePassword != null
                && !keyStorePassword.isBlank()) {

              // --- HTTPS (SSL) Connector ---
              org.eclipse.jetty.util.ssl.SslContextFactory.Server sslContextFactory =
                  new org.eclipse.jetty.util.ssl.SslContextFactory.Server();
              sslContextFactory.setKeyStorePath(keyStorePath);
              sslContextFactory.setKeyStorePassword(keyStorePassword);
              sslContextFactory.setKeyStoreType(keyStoreType);
              if (keyAlias != null && !keyAlias.isBlank()) {
                sslContextFactory.setCertAlias(keyAlias);
              }

              HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
              httpsConfig.addCustomizer(new org.eclipse.jetty.server.SecureRequestCustomizer());

              // Only HTTPS
              connector =
                  new ServerConnector(
                      server, sslContextFactory, new HttpConnectionFactory(httpsConfig));
              connector.setPort(httpsPort);
              log.info("HTTPS enabled on port {} using keystore: {}", httpsPort, keyStorePath);
            } else {
              // Only HTTP
              connector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
              connector.setPort(httpPort);
              log.info("HTTP only enabled on port {}", httpPort);
            }

            // --- Set connectors (HTTP or HTTPS only) ---
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
