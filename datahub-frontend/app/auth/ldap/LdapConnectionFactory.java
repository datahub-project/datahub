package auth.ldap;

import java.util.Hashtable;
import javax.annotation.Nonnull;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory class for creating and managing LDAP connections using JNDI (Java Naming and Directory
 * Interface).
 *
 * <p>This factory handles all aspects of LDAP connection management including:
 *
 * <ul>
 *   <li><b>Authentication:</b> Supports both service account and user credential authentication
 *   <li><b>SSL/TLS:</b> Configurable secure connections with certificate validation
 *   <li><b>Connection Pooling:</b> Automatic connection pooling for performance
 *   <li><b>Timeouts:</b> Configurable connection and read timeouts
 *   <li><b>Active Directory Support:</b> Binary attribute handling for AD-specific attributes
 * </ul>
 *
 * <p><b>Usage Patterns:</b>
 *
 * <pre>
 * // Pattern 1: Using configured service account (requires bindDn/bindPassword)
 * LdapConnectionFactory factory = new LdapConnectionFactory(configs);
 * DirContext ctx = factory.createConnection();
 * try {
 *     // Perform LDAP operations
 * } finally {
 *     factory.closeConnection(ctx);
 * }
 *
 * // Pattern 2: Using user's own credentials (more secure)
 * DirContext ctx = factory.createConnection("jdoe@example.com", "userPassword");
 * try {
 *     // Perform LDAP operations as the user
 * } finally {
 *     factory.closeConnection(ctx);
 * }
 * </pre>
 *
 * <p><b>Configuration:</b> This factory reads configuration from {@link LdapConfigs}, which loads
 * settings from application.conf or environment variables:
 *
 * <ul>
 *   <li>{@code auth.ldap.server} - LDAP server URL (e.g., "ldaps://ldap.example.com:636")
 *   <li>{@code auth.ldap.bindDn} - Service account DN (optional if using user credentials)
 *   <li>{@code auth.ldap.bindPassword} - Service account password (optional)
 *   <li>{@code auth.ldap.useSsl} - Enable SSL/TLS
 *   <li>{@code auth.ldap.connectionTimeout} - Connection timeout in milliseconds
 *   <li>{@code auth.ldap.readTimeout} - Read timeout in milliseconds
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. Multiple threads can safely create
 * connections using the same factory instance.
 *
 * <p><b>Connection Pooling:</b> JNDI automatically pools connections when using the same
 * credentials. This improves performance by reusing existing connections.
 *
 * @see LdapConfigs
 * @see javax.naming.directory.DirContext
 */
@Slf4j
public class LdapConnectionFactory {

  /** LDAP configuration containing server URL, credentials, and connection settings. */
  private final LdapConfigs configs;

  /**
   * Constructs a new LdapConnectionFactory with the specified configuration.
   *
   * @param configs The LDAP configuration to use for creating connections
   * @throws NullPointerException if configs is null
   */
  public LdapConnectionFactory(@Nonnull LdapConfigs configs) {
    this.configs = configs;
  }

  /**
   * Creates an authenticated LDAP connection using the configured service account credentials.
   *
   * <p>This method uses the bindDn and bindPassword from the configuration to authenticate to LDAP.
   * It's typically used for administrative operations or when user credentials are not available.
   *
   * <p><b>When to use:</b>
   *
   * <ul>
   *   <li>Batch operations that need to access multiple users' data
   *   <li>Administrative tasks like user synchronization
   *   <li>When user credentials are not available (e.g., background jobs)
   * </ul>
   *
   * <p><b>Security Note:</b> This requires storing service account credentials in configuration,
   * which may not be ideal for security. Consider using {@link #createConnection(String, String)}
   * with user credentials when possible.
   *
   * <p><b>Configuration Required:</b>
   *
   * <ul>
   *   <li>{@code auth.ldap.bindDn} - Service account DN
   *   <li>{@code auth.ldap.bindPassword} - Service account password
   * </ul>
   *
   * @return An authenticated DirContext ready for LDAP operations
   * @throws NamingException if connection fails, authentication fails, or bindDn/bindPassword are
   *     not configured
   * @see #createConnection(String, String)
   */
  public DirContext createConnection() throws NamingException {
    return createConnection(configs.getBindDn(), configs.getBindPassword());
  }

  /**
   * Creates an LDAP connection authenticated with specific credentials.
   *
   * <p>This method allows authentication with any set of credentials, making it suitable for:
   *
   * <ul>
   *   <li><b>User authentication:</b> Verify user credentials by attempting to bind
   *   <li><b>User-scoped operations:</b> Perform LDAP searches with user's own credentials
   *   <li><b>Least privilege:</b> Each user can only access their own LDAP data
   * </ul>
   *
   * <p><b>Principal Formats Supported:</b>
   *
   * <ul>
   *   <li><b>UPN:</b> "jdoe@example.com" (User Principal Name - common in Active Directory)
   *   <li><b>DN:</b> "CN=John Doe,OU=Users,DC=example,DC=com" (Distinguished Name)
   *   <li><b>Simple:</b> "jdoe" (simple username - depends on LDAP server configuration)
   * </ul>
   *
   * <p><b>Connection Configuration:</b> This method configures the LDAP connection with:
   *
   * <ul>
   *   <li><b>Authentication:</b> Simple bind with provided credentials
   *   <li><b>SSL/TLS:</b> Enabled if configured (recommended for production)
   *   <li><b>Connection Pooling:</b> Automatic pooling for same credentials
   *   <li><b>Timeouts:</b> Configurable connection and read timeouts
   *   <li><b>Binary Attributes:</b> Support for AD objectSID and objectGUID
   *   <li><b>Referrals:</b> Automatically follows LDAP referrals
   * </ul>
   *
   * <p><b>Example Usage:</b>
   *
   * <pre>
   * // Authenticate user
   * try {
   *     DirContext ctx = factory.createConnection("jdoe@example.com", "password");
   *     // Authentication successful
   *     factory.closeConnection(ctx);
   * } catch (AuthenticationException e) {
   *     // Invalid credentials
   * }
   *
   * // Perform user-scoped search
   * DirContext ctx = factory.createConnection(userUpn, userPassword);
   * try {
   *     // Search for user's own attributes
   *     NamingEnumeration results = ctx.search(...);
   * } finally {
   *     factory.closeConnection(ctx);
   * }
   * </pre>
   *
   * <p><b>Error Codes:</b> Common LDAP error codes you might encounter:
   *
   * <ul>
   *   <li><b>49:</b> Invalid credentials (wrong username or password)
   *   <li><b>52e:</b> Invalid credentials (Active Directory specific)
   *   <li><b>525:</b> User not found
   *   <li><b>530:</b> Not permitted to logon at this time
   *   <li><b>531:</b> Not permitted to logon at this workstation
   *   <li><b>532:</b> Password expired
   *   <li><b>533:</b> Account disabled
   *   <li><b>701:</b> Account expired
   *   <li><b>773:</b> User must reset password
   * </ul>
   *
   * @param principal The DN, UPN, or username to authenticate with (e.g., "jdoe@example.com")
   * @param credentials The password for authentication
   * @return An authenticated DirContext ready for LDAP operations
   * @throws NamingException if connection fails or authentication fails
   * @throws javax.naming.AuthenticationException if credentials are invalid
   * @throws javax.naming.CommunicationException if cannot connect to LDAP server
   */
  public DirContext createConnection(@Nonnull String principal, @Nonnull String credentials)
      throws NamingException {

    Hashtable<String, String> env = new Hashtable<>();

    // Set the LDAP provider
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, configs.getServer());

    // Set authentication type
    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_PRINCIPAL, principal);
    env.put(Context.SECURITY_CREDENTIALS, credentials);

    // SSL/TLS Configuration
    if (configs.isUseSsl()) {
      env.put(Context.SECURITY_PROTOCOL, "ssl");

      if (configs.isTrustAllCerts()) {
        log.warn(
            "LDAP is configured to trust all certificates. This should only be used for testing!");
        // Note: Trusting all certs requires additional SSL socket factory configuration
        // For production, proper certificate validation should be used
      }
    }

    if (configs.isStartTls()) {
      env.put("java.naming.ldap.starttls.enable", "true");
    }

    // Connection pooling
    env.put("com.sun.jndi.ldap.connect.pool", "true");
    env.put("com.sun.jndi.ldap.connect.pool.timeout", String.valueOf(configs.getPoolTimeout()));

    // Timeouts (configurable)
    env.put("com.sun.jndi.ldap.connect.timeout", String.valueOf(configs.getConnectionTimeout()));
    env.put("com.sun.jndi.ldap.read.timeout", String.valueOf(configs.getReadTimeout()));

    // Binary attributes for Active Directory (objectSID, objectGUID)
    env.put("java.naming.ldap.attributes.binary", "objectSID objectGUID");

    // Referral handling
    env.put(Context.REFERRAL, "follow");

    log.debug("Creating LDAP connection to {} with principal {}", configs.getServer(), principal);

    try {
      DirContext ctx = new InitialDirContext(env);
      log.debug("Successfully created LDAP connection");
      return ctx;
    } catch (NamingException e) {
      log.error("Failed to create LDAP connection: {}", e.getMessage());
      throw e;
    }
  }

  /**
   * Closes an LDAP connection safely.
   *
   * <p>This method should always be called in a finally block to ensure connections are properly
   * released back to the pool or closed.
   *
   * <p><b>Best Practice:</b>
   *
   * <pre>
   * DirContext ctx = null;
   * try {
   *     ctx = factory.createConnection();
   *     // Perform LDAP operations
   * } finally {
   *     factory.closeConnection(ctx);  // Always close in finally block
   * }
   * </pre>
   *
   * <p><b>Null-Safe:</b> This method safely handles null contexts, so you don't need to check
   * before calling.
   *
   * <p><b>Connection Pooling:</b> When connection pooling is enabled (which it is by default), this
   * method returns the connection to the pool rather than actually closing it, improving
   * performance.
   *
   * @param ctx The DirContext to close (can be null)
   */
  public void closeConnection(DirContext ctx) {
    if (ctx != null) {
      try {
        ctx.close();
        log.debug("LDAP connection closed successfully");
      } catch (NamingException e) {
        log.warn("Error closing LDAP connection: {}", e.getMessage());
      }
    }
  }

  /**
   * Tests if an LDAP connection can be established with the configured settings.
   *
   * <p>This method is useful for:
   *
   * <ul>
   *   <li><b>Configuration validation:</b> Verify LDAP settings during startup
   *   <li><b>Health checks:</b> Monitor LDAP server availability
   *   <li><b>Troubleshooting:</b> Diagnose connection issues
   * </ul>
   *
   * <p><b>What it tests:</b>
   *
   * <ul>
   *   <li>LDAP server is reachable
   *   <li>SSL/TLS configuration is correct (if enabled)
   *   <li>Service account credentials are valid (if configured)
   *   <li>Network connectivity is working
   * </ul>
   *
   * <p><b>Note:</b> This method uses the configured bindDn/bindPassword. If these are not
   * configured, the test will fail.
   *
   * <p><b>Example Usage:</b>
   *
   * <pre>
   * LdapConnectionFactory factory = new LdapConnectionFactory(configs);
   * if (factory.testConnection()) {
   *     log.info("LDAP connection test successful");
   * } else {
   *     log.error("LDAP connection test failed - check configuration");
   * }
   * </pre>
   *
   * @return true if connection test succeeds, false if it fails
   */
  public boolean testConnection() {
    DirContext ctx = null;
    try {
      ctx = createConnection();
      return true;
    } catch (NamingException e) {
      log.error("LDAP connection test failed: {}", e.getMessage());
      return false;
    } finally {
      closeConnection(ctx);
    }
  }
}
