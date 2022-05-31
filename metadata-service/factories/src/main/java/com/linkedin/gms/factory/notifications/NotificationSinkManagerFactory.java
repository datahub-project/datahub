package com.linkedin.gms.factory.notifications;

import com.datahub.authentication.Authentication;
import com.datahub.notification.provider.SecretProvider;
import com.datahub.notification.provider.SettingsProvider;
import com.datahub.notification.provider.IdentityProvider;
import com.datahub.notification.NotificationSink;
import com.datahub.notification.NotificationSinkConfig;
import com.datahub.notification.NotificationSinkManager;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.spring.YamlPropertySourceFactory;
import com.linkedin.metadata.config.notification.NotificationSinkConfiguration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
public class NotificationSinkManagerFactory {

  @Autowired
  @Qualifier("restliEntityClient")
  private EntityClient entityClient;

  @Autowired
  @Qualifier("systemAuthentication")
  private Authentication systemAuthentication;

  @Autowired
  @Qualifier("settingsProvider")
  private SettingsProvider settingsProvider;

  @Autowired
  @Qualifier("identityProvider")
  private IdentityProvider identityProvider;

  @Autowired
  @Qualifier("secretProvider")
  private SecretProvider secretProvider;

  @Autowired
  private ConfigurationProvider configurationProvider;

  @Bean(name = "notificationSinkManager")
  @Singleton
  @Nonnull
  protected NotificationSinkManager getInstance() {
    boolean isNotificationsEnabled = this.configurationProvider.getNotifications().isEnabled();
    String baseUrl = this.configurationProvider.getBaseUrl();

    final List<NotificationSink> configuredSinks = new ArrayList<>();
    if (isNotificationsEnabled) {
      final List<NotificationSinkConfiguration> sinkConfigurations = this.configurationProvider.getNotifications().getSinks();
      for (NotificationSinkConfiguration sink : sinkConfigurations) {

        boolean isSinkEnabled = sink.isEnabled();

        if (isSinkEnabled) {
          final String type = sink.getType();
          final Map<String, Object> configs = sink.getConfigs() != null ? sink.getConfigs() : Collections.emptyMap();

          log.debug(String.format("Found configs for notification sink of type %s: %s ", type, configs));

          // Instantiate the Notification Sink.
          Class<? extends NotificationSink> clazz = null;
          try {
            clazz = (Class<? extends NotificationSink>) Class.forName(type);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(
                String.format("Failed to find NotificationSink class with name %s on the classpath.", type));
          }

          // Else construct an instance of the class, each class should have an empty constructor.
          try {
            final NotificationSink notificationSink = clazz.newInstance();
            notificationSink.init(new NotificationSinkConfig(
                configs,
                this.settingsProvider,
                this.identityProvider,
                this.secretProvider,
                baseUrl
            ));
            configuredSinks.add(notificationSink);
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to instantiate NotificationSink with class name %s", clazz.getCanonicalName()), e);
          }
        } else {
          log.info(String.format("Skipping disabled notification sink with type %s", sink.getType()));
        }
      }
      log.info(String.format("Creating NotificationSinkManager in ENABLED mode. sinks: %s", configuredSinks));
      return new NotificationSinkManager(NotificationSinkManager.NotificationManagerMode.ENABLED, configuredSinks);
    }
    // Notifications are disabled.
    log.info("Creating NotificationSinkManager in DISABLED mode.");
    return new NotificationSinkManager(
        NotificationSinkManager.NotificationManagerMode.DISABLED,
        Collections.emptyList());
  }
}