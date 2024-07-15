package com.datahub.notification;

import com.datahub.notification.provider.EntityNameProvider;
import com.datahub.notification.provider.IdentityProvider;
import com.datahub.notification.provider.SecretProvider;
import com.datahub.notification.provider.SettingsProvider;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.connection.ConnectionService;
import com.linkedin.metadata.integration.IntegrationsService;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

@Data
@AllArgsConstructor
@Getter
public class NotificationSinkConfig {
  /** Static configuration for a notification sink provided via application.yaml at boot time. */
  private final Map<String, Object> staticConfig;

  /** Entity client, exposing core API to notification sinks */
  private final EntityClient entityClient;

  /** Settings provider, which is responsible for resolving platform settings. */
  private final SettingsProvider settingsProvider;

  /** User provider, which is responsible for resolving user to their contact info attributes. */
  private final IdentityProvider identityProvider;

  /** Entity name provider, which is responsible for resolving names of entities such as groups. */
  private final EntityNameProvider entityNameProvider;

  /** Secret provider, which is responsible for resolving secret values from their urn. */
  private final SecretProvider secretProvider;

  /** A provider of connection information. */
  private final ConnectionService connectionService;

  /** A provider of access to the integrations service. */
  private final IntegrationsService integrationsService;

  /** The base URL where DataHub is deployed. Used to construct URL strings. */
  private final String baseUrl;
}
