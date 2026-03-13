package auth.ldap;

import com.typesafe.config.Config;
import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * Configuration class for LDAP authentication and provisioning. Reads configuration from
 * environment variables and application config.
 */
@Getter
public class LdapConfigs {
  private static final String LDAP_ENABLED_CONFIG_PATH = "auth.ldap.enabled";
  private static final String LDAP_SERVER_CONFIG_PATH = "auth.ldap.server";
  private static final String LDAP_BASE_DN_CONFIG_PATH = "auth.ldap.baseDn";
  private static final String LDAP_BIND_DN_CONFIG_PATH = "auth.ldap.bindDn";
  private static final String LDAP_BIND_PASSWORD_CONFIG_PATH = "auth.ldap.bindPassword";
  private static final String LDAP_USER_FILTER_CONFIG_PATH = "auth.ldap.userFilter";
  private static final String LDAP_USER_ID_ATTRIBUTE_CONFIG_PATH = "auth.ldap.userIdAttribute";

  // SSL/TLS Configuration
  private static final String LDAP_USE_SSL_CONFIG_PATH = "auth.ldap.useSsl";
  private static final String LDAP_START_TLS_CONFIG_PATH = "auth.ldap.startTls";
  private static final String LDAP_TRUST_ALL_CERTS_CONFIG_PATH = "auth.ldap.trustAllCerts";

  // Connection Timeout Configuration
  private static final String LDAP_CONNECTION_TIMEOUT_CONFIG_PATH = "auth.ldap.connectionTimeout";
  private static final String LDAP_READ_TIMEOUT_CONFIG_PATH = "auth.ldap.readTimeout";
  private static final String LDAP_POOL_TIMEOUT_CONFIG_PATH = "auth.ldap.poolTimeout";

  // User Attribute Mapping
  private static final String LDAP_USER_FIRST_NAME_ATTRIBUTE_CONFIG_PATH =
      "auth.ldap.userFirstNameAttribute";
  private static final String LDAP_USER_LAST_NAME_ATTRIBUTE_CONFIG_PATH =
      "auth.ldap.userLastNameAttribute";
  private static final String LDAP_USER_EMAIL_ATTRIBUTE_CONFIG_PATH =
      "auth.ldap.userEmailAttribute";
  private static final String LDAP_USER_DISPLAY_NAME_ATTRIBUTE_CONFIG_PATH =
      "auth.ldap.userDisplayNameAttribute";

  // Provisioning Configuration
  private static final String LDAP_JIT_PROVISIONING_ENABLED_CONFIG_PATH =
      "auth.ldap.jitProvisioningEnabled";
  private static final String LDAP_PRE_PROVISIONING_REQUIRED_CONFIG_PATH =
      "auth.ldap.preProvisioningRequired";

  // Group Configuration
  private static final String LDAP_EXTRACT_GROUPS_ENABLED_CONFIG_PATH =
      "auth.ldap.extractGroupsEnabled";
  private static final String LDAP_GROUP_BASE_DN_CONFIG_PATH = "auth.ldap.groupBaseDn";
  private static final String LDAP_GROUP_FILTER_CONFIG_PATH = "auth.ldap.groupFilter";
  private static final String LDAP_GROUP_NAME_ATTRIBUTE_CONFIG_PATH =
      "auth.ldap.groupNameAttribute";

  private final Config config;

  // Basic LDAP Configuration
  private final boolean enabled;
  private final String server;
  private final String baseDn;
  private final String bindDn;
  private final String bindPassword;
  private final String userFilter;
  private final String userIdAttribute;

  // SSL/TLS Configuration
  private final boolean useSsl;
  private final boolean startTls;
  private final boolean trustAllCerts;

  // Connection Timeout Configuration
  private final int connectionTimeout;
  private final int readTimeout;
  private final int poolTimeout;

  // User Attribute Mapping
  private final String userFirstNameAttribute;
  private final String userLastNameAttribute;
  private final String userEmailAttribute;
  private final String userDisplayNameAttribute;

  // Provisioning Configuration
  private final boolean jitProvisioningEnabled;
  private final boolean preProvisioningRequired;

  // Group Configuration
  private final boolean extractGroupsEnabled;
  private final String groupBaseDn;
  private final String groupFilter;
  private final String groupNameAttribute;

  public LdapConfigs(@Nonnull final Config config) {
    this.config = config;

    // Basic LDAP Configuration
    this.enabled = getBoolean(LDAP_ENABLED_CONFIG_PATH, false);
    this.server = getString(LDAP_SERVER_CONFIG_PATH, "");
    this.baseDn = getString(LDAP_BASE_DN_CONFIG_PATH, "");
    this.bindDn = getString(LDAP_BIND_DN_CONFIG_PATH, "");
    this.bindPassword = getString(LDAP_BIND_PASSWORD_CONFIG_PATH, "");
    this.userFilter = getString(LDAP_USER_FILTER_CONFIG_PATH, "(uid={0})");
    this.userIdAttribute = getString(LDAP_USER_ID_ATTRIBUTE_CONFIG_PATH, "uid");

    // SSL/TLS Configuration
    this.useSsl = getBoolean(LDAP_USE_SSL_CONFIG_PATH, false);
    this.startTls = getBoolean(LDAP_START_TLS_CONFIG_PATH, false);
    this.trustAllCerts = getBoolean(LDAP_TRUST_ALL_CERTS_CONFIG_PATH, false);

    // Connection Timeout Configuration (in milliseconds)
    this.connectionTimeout = getInt(LDAP_CONNECTION_TIMEOUT_CONFIG_PATH, 5000);
    this.readTimeout = getInt(LDAP_READ_TIMEOUT_CONFIG_PATH, 5000);
    this.poolTimeout = getInt(LDAP_POOL_TIMEOUT_CONFIG_PATH, 300000);

    // User Attribute Mapping
    this.userFirstNameAttribute =
        getString(LDAP_USER_FIRST_NAME_ATTRIBUTE_CONFIG_PATH, "givenName");
    this.userLastNameAttribute = getString(LDAP_USER_LAST_NAME_ATTRIBUTE_CONFIG_PATH, "sn");
    this.userEmailAttribute = getString(LDAP_USER_EMAIL_ATTRIBUTE_CONFIG_PATH, "mail");
    this.userDisplayNameAttribute =
        getString(LDAP_USER_DISPLAY_NAME_ATTRIBUTE_CONFIG_PATH, "displayName");

    // Provisioning Configuration
    this.jitProvisioningEnabled = getBoolean(LDAP_JIT_PROVISIONING_ENABLED_CONFIG_PATH, true);
    this.preProvisioningRequired = getBoolean(LDAP_PRE_PROVISIONING_REQUIRED_CONFIG_PATH, false);

    // Group Configuration
    this.extractGroupsEnabled = getBoolean(LDAP_EXTRACT_GROUPS_ENABLED_CONFIG_PATH, false);
    this.groupBaseDn = getString(LDAP_GROUP_BASE_DN_CONFIG_PATH, "");
    this.groupFilter = getString(LDAP_GROUP_FILTER_CONFIG_PATH, "(member={0})");
    this.groupNameAttribute = getString(LDAP_GROUP_NAME_ATTRIBUTE_CONFIG_PATH, "cn");
  }

  private String getString(String path, String defaultValue) {
    if (config.hasPath(path)) {
      return config.getString(path);
    }
    // Try environment variable (convert path to env var format)
    String envVar = pathToEnvVar(path);
    String envValue = System.getenv(envVar);
    return envValue != null ? envValue : defaultValue;
  }

  private boolean getBoolean(String path, boolean defaultValue) {
    if (config.hasPath(path)) {
      return config.getBoolean(path);
    }
    // Try environment variable
    String envVar = pathToEnvVar(path);
    String envValue = System.getenv(envVar);
    return envValue != null ? Boolean.parseBoolean(envValue) : defaultValue;
  }

  private int getInt(String path, int defaultValue) {
    if (config.hasPath(path)) {
      return config.getInt(path);
    }
    // Try environment variable
    String envVar = pathToEnvVar(path);
    String envValue = System.getenv(envVar);
    try {
      return envValue != null ? Integer.parseInt(envValue) : defaultValue;
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Converts a config path to environment variable format. Example: auth.ldap.enabled ->
   * AUTH_LDAP_ENABLED
   */
  private String pathToEnvVar(String path) {
    return path.replace(".", "_").toUpperCase();
  }
}
