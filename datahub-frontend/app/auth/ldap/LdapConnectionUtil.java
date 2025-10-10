package auth.ldap;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for creating LDAP connections with common configuration. Centralizes LDAP
 * environment setup to avoid code duplication.
 */
public class LdapConnectionUtil {
  private static final Logger log = LoggerFactory.getLogger(LdapConnectionUtil.class);

  private LdapConnectionUtil() {} // Prevent instantiation

  /**
   * Creates a configured LDAP environment hashtable based on the provided options.
   *
   * @param options LDAP configuration options
   * @param username The username for authentication (used to replace {USERNAME} placeholder)
   * @param password The password for authentication
   * @return Configured LDAP environment hashtable
   */
  public static Hashtable<String, Object> createLdapEnvironment(
      @Nonnull Map<String, String> options, @Nonnull String username, @Nonnull String password) {

    Hashtable<String, Object> env = new Hashtable<>();

    // Basic LDAP configuration
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, options.getOrDefault("groupProvider", ""));
    env.put(
        Context.SECURITY_AUTHENTICATION,
        options.getOrDefault("java.naming.security.authentication", "simple"));

    // Replace {USERNAME} placeholder in authIdentity with actual username
    String authIdentityTemplate = options.getOrDefault("authIdentity", "");
    String authIdentity = authIdentityTemplate.replace("{USERNAME}", username);
    env.put(Context.SECURITY_PRINCIPAL, authIdentity);
    env.put(Context.SECURITY_CREDENTIALS, password);

    // Connection settings
    env.put("java.naming.ldap.attributes.binary", "objectSID objectGUID");
    env.put(Context.REFERRAL, "follow");
    env.put("com.sun.jndi.ldap.connect.timeout", options.getOrDefault("connectTimeout", "5000"));
    env.put("com.sun.jndi.ldap.read.timeout", options.getOrDefault("readTimeout", "5000"));

    // SSL configuration
    if ("true".equals(options.get("useSSL"))) {
      env.put("java.naming.security.protocol", "ssl");
    }

    // Connection pooling
    env.put("com.sun.jndi.ldap.connect.pool", "true");

    // Debug tracing (optional)
    env.put("com.sun.jndi.ldap.trace.ber", System.err);

    return env;
  }

  /**
   * Creates an LDAP directory context using the provided environment.
   *
   * @param env LDAP environment configuration
   * @return DirContext for LDAP operations
   * @throws NamingException if context creation fails
   */
  public static DirContext createLdapContext(Hashtable<String, Object> env) throws NamingException {
    return new InitialDirContext(env);
  }

  /**
   * Logs LDAP environment configuration for debugging purposes. Masks sensitive information like
   * passwords.
   *
   * @param env LDAP environment configuration
   */
  public static void logLdapEnvironment(Hashtable<String, Object> env) {
    log.info("LDAP Configuration Debug:");
    log.info("  Provider URL: {}", env.get(Context.PROVIDER_URL));
    log.info("  Security Principal: {}", env.get(Context.SECURITY_PRINCIPAL));
    log.info("  Security Authentication: {}", env.get(Context.SECURITY_AUTHENTICATION));
    log.info("  Security Protocol: {}", env.get("java.naming.security.protocol"));

    log.info("Complete LDAP Environment Parameters:");
    for (Object key : env.keySet()) {
      Object value = env.get(key);
      if (key.toString().toLowerCase().contains("password")
          || key.toString().toLowerCase().contains("credentials")) {
        log.info("  {} = [HIDDEN]", key);
      } else {
        log.info("  {} = {}", key, value);
      }
    }
  }

  /**
   * Extracts LDAP configuration options from the JAAS configuration file. This approach avoids
   * reflection issues with Java's module system.
   *
   * @return Map containing LDAP configuration options
   */
  public static Map<String, String> extractLdapOptionsFromJaasConfig() {
    Map<String, String> ldapOptions = new HashMap<>();

    String jaasConfigFile = System.getProperty("java.security.auth.login.config");
    if (jaasConfigFile == null) {
      log.warn(
          "JAAS configuration file path not set in system property: java.security.auth.login.config");
      return ldapOptions;
    }

    try (java.io.BufferedReader reader =
        new java.io.BufferedReader(new java.io.FileReader(jaasConfigFile))) {
      String line;
      boolean inLdapModule = false;
      boolean inWhzAuthentication = false;

      // Pattern to match key="value" or key=value
      java.util.regex.Pattern optionPattern =
          java.util.regex.Pattern.compile("\\s*(\\w+)\\s*=\\s*\"?([^\"\\s;]+)\"?\\s*;?");

      while ((line = reader.readLine()) != null) {
        line = line.trim();

        // Check if we're entering the WHZ-Authentication block
        if (line.startsWith("WHZ-Authentication")) {
          inWhzAuthentication = true;
          continue;
        }

        // Check if we're entering the LdapLoginModule
        if (inWhzAuthentication && line.contains("com.sun.security.auth.module.LdapLoginModule")) {
          inLdapModule = true;
          continue;
        }

        // Check if we're exiting the current module (end of block)
        if (line.equals("};") || line.startsWith("}")) {
          inWhzAuthentication = false;
          inLdapModule = false;
          continue;
        }

        // Parse options within the LdapLoginModule
        if (inLdapModule && !line.isEmpty() && !line.startsWith("//")) {
          java.util.regex.Matcher matcher = optionPattern.matcher(line);
          if (matcher.find()) {
            String key = matcher.group(1);
            String value = matcher.group(2);
            ldapOptions.put(key, value);
            log.debug("Loaded LDAP option: {} = {}", key, value);
          }
        }
      }

      log.info("Successfully loaded {} LDAP options from JAAS configuration", ldapOptions.size());

    } catch (java.io.IOException e) {
      log.error("Failed to read JAAS configuration file: {}", jaasConfigFile, e);
    }

    return ldapOptions;
  }
}
