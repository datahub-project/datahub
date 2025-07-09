package com.linkedin.gms.factory.notifications;

import com.datahub.notification.NotificationSink;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationSinkManager;
import com.datahub.notification.provider.EntityNameProvider;
import com.datahub.notification.provider.IdentityProvider;
import com.datahub.notification.provider.SecretProvider;
import com.datahub.notification.provider.SettingsProvider;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.connection.ConnectionServiceFactory;
import com.linkedin.metadata.config.notification.NotificationSinkConfiguration;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.integration.IntegrationsService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Slf4j
@Configuration
@Import({
  SettingsProviderFactory.class,
  IdentityProviderFactory.class,
  SecretProviderFactory.class,
  ConnectionServiceFactory.class
})
public class NotificationSinkManagerFactory {
  @Autowired
  @Qualifier("settingsProvider")
  private SettingsProvider settingsProvider;

  @Autowired
  @Qualifier("identityProvider")
  private IdentityProvider identityProvider;

  @Autowired
  @Qualifier("systemEntityClient")
  private SystemEntityClient entityClient;

  @Autowired
  @Qualifier("secretProvider")
  private SecretProvider secretProvider;

  @Autowired
  @Qualifier("connectionService")
  private ConnectionService connectionService;

  @Autowired
  @Qualifier("integrationsService")
  private IntegrationsService integrationsService;

  @Autowired private ConfigurationProvider configurationProvider;

  @Bean(name = "notificationSinkManager")
  @Nonnull
  protected NotificationSinkManager getInstance(
      @Qualifier("systemOperationContext") OperationContext systemOpContext) {
    boolean isNotificationsEnabled = this.configurationProvider.getNotifications().isEnabled();
    String baseUrl = this.configurationProvider.getBaseUrl();

    EntityNameProvider entityNameProvider = new EntityNameProvider(this.entityClient);

    final List<NotificationSink> configuredSinks = new ArrayList<>();
    if (isNotificationsEnabled) {
      final List<NotificationSinkConfiguration> sinkConfigurations =
          this.configurationProvider.getNotifications().getSinks();
      for (NotificationSinkConfiguration sink : sinkConfigurations) {

        boolean isSinkEnabled = sink.isEnabled();

        if (isSinkEnabled) {
          final String type = sink.getType();
          final Map<String, Object> configs =
              sink.getConfigs() != null ? sink.getConfigs() : Collections.emptyMap();

          log.debug(
              String.format("Found configs for notification sink of type %s: %s ", type, configs));

          // Instantiate the Notification Sink.
          Class<? extends NotificationSink> clazz = null;
          try {
            clazz = (Class<? extends NotificationSink>) Class.forName(type);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                String.format(
                    "Failed to find NotificationSink class with name %s on the classpath.", type));
          }

          // Else construct an instance of the class, each class should have an empty constructor.
          try {
            final NotificationSink notificationSink = clazz.newInstance();
            notificationSink.init(
                systemOpContext,
                new NotificationSinkConfig(
                    configs,
                    this.entityClient,
                    this.settingsProvider,
                    this.identityProvider,
                    entityNameProvider,
                    this.secretProvider,
                    this.connectionService,
                    this.integrationsService,
                    baseUrl));
            configuredSinks.add(notificationSink);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to instantiate NotificationSink with class name %s",
                    clazz.getCanonicalName()),
                e);
          }
        } else {
          log.info(
              String.format("Skipping disabled notification sink with type %s", sink.getType()));
        }
      }
      log.info(
          String.format(
              "Creating NotificationSinkManager in ENABLED mode. sinks: %s", configuredSinks));
      return new NotificationSinkManager(
          NotificationSinkManager.NotificationManagerMode.ENABLED, configuredSinks);
    }
    // Notifications are disabled.
    log.info("Creating NotificationSinkManager in DISABLED mode.");
    return new NotificationSinkManager(
        NotificationSinkManager.NotificationManagerMode.DISABLED, Collections.emptyList());
  }
}
