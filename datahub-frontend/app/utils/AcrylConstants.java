package utils;

import com.linkedin.util.Configuration;

/** SaaS only - Constants related to Acryl-only features. */
public class AcrylConstants {

  public static final String INTEGRATIONS_HOST_ENV_VAR = "DATAHUB_INTEGRATIONS_HOST";
  public static final String INTEGRATIONS_PORT_ENV_VAR = "DATAHUB_INTEGRATIONS_PORT";
  public static final String INTEGRATIONS_USE_SSL_ENV_VAR = "DATAHUB_INTEGRATIONS_USE_SSL";
  public static final String INTEGRATIONS_SSL_PROTOCOL_VAR = "DATAHUB_INTEGRATIONS_SSL_PROTOCOL";

  public static final String INTEGRATIONS_SERVICE_HOST_CONFIG_PATH = "integrationsService.host";
  public static final String INTEGRATIONS_SERVICE_PORT_CONFIG_PATH = "integrationsService.port";
  public static final String INTEGRATIONS_SERVICE_USE_SSL_CONFIG_PATH =
      "integrationsService.useSsl";
  public static final String INTEGRATIONS_SERVICE_SSL_PROTOCOL_CONFIG_PATH =
      "integrationsService.sslProtocol";

  public static final String DEFAULT_INTEGRATIONS_SERVICE_HOST =
      Configuration.getEnvironmentVariable(INTEGRATIONS_HOST_ENV_VAR, "localhost");
  public static final Integer DEFAULT_INTEGRATIONS_SERVICE_PORT =
      Integer.parseInt(Configuration.getEnvironmentVariable(INTEGRATIONS_PORT_ENV_VAR, "9003"));
  public static final Boolean DEFAULT_INTEGRATIONS_SERVICE_USE_SSL =
      Boolean.parseBoolean(
          Configuration.getEnvironmentVariable(INTEGRATIONS_USE_SSL_ENV_VAR, "False"));
  public static final String DEFAULT_INTEGRATIONS_SERVICE_SSL_PROTOCOL =
      Configuration.getEnvironmentVariable(INTEGRATIONS_SSL_PROTOCOL_VAR);

  private AcrylConstants() {}
}
