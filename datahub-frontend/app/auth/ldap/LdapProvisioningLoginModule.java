package auth.ldap;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import lombok.extern.slf4j.Slf4j;
import security.DataHubUserPrincipal;

/**
 * JAAS LoginModule that provisions users and groups in DataHub after successful LDAP
 * authentication.
 *
 * <p>This module is designed to run as part of a JAAS authentication chain, specifically AFTER the
 * {@code com.sun.security.auth.module.LdapLoginModule} has successfully authenticated the user
 * against LDAP. It extracts user information and group memberships from LDAP and provisions them in
 * DataHub.
 *
 * <p><b>Key Features:</b>
 *
 * <ul>
 *   <li><b>Secure Credential Handling:</b> Uses the user's own password (extracted from JAAS
 *       sharedState) to connect to LDAP, eliminating the need to store service account credentials
 *   <li><b>JIT Provisioning:</b> Automatically creates/updates user profiles in DataHub during
 *       login
 *   <li><b>Group Synchronization:</b> Extracts and provisions LDAP groups and group memberships
 *   <li><b>Flexible Configuration:</b> Supports both JIT provisioning and pre-provisioning modes
 * </ul>
 *
 * <p><b>JAAS Configuration Example:</b>
 *
 * <pre>
 * WHZ-Authentication {
 *   // Optional: File-based authentication for admin users
 *   security.PropertyFileLoginModule sufficient
 *     debug="true"
 *     file="/path/to/user.props";
 *
 *   // Required: LDAP authentication
 *   com.sun.security.auth.module.LdapLoginModule required
 *     userProvider="ldaps://ldap.example.com:636/DC=example,DC=com"
 *     authIdentity="{USERNAME}@example.com"
 *     userFilter="(&(userPrincipalName={USERNAME}@example.com)(objectClass=person))"
 *     storePass="true"     // IMPORTANT: Store password in sharedState
 *     debug="true"
 *     useSSL="true";
 *
 *   // Required: DataHub provisioning (this module)
 *   auth.ldap.LdapProvisioningLoginModule required
 *     debug="true"
 *     tryFirstPass="true";  // Use password from sharedState
 * };
 * </pre>
 *
 * <p><b>JAAS Lifecycle:</b>
 *
 * <ol>
 *   <li>{@link #initialize} - Called once to set up the module
 *   <li>{@link #login} - Called during authentication phase (returns true immediately)
 *   <li>{@link #commit} - Called after successful authentication (performs provisioning)
 *   <li>{@link #abort} - Called if authentication fails (cleans up state)
 *   <li>{@link #logout} - Called when user logs out (removes principals)
 * </ol>
 *
 * <p><b>Configuration Requirements:</b>
 *
 * <ul>
 *   <li>{@code auth.ldap.enabled=true} - Enable LDAP authentication
 *   <li>{@code auth.ldap.server} - LDAP server URL
 *   <li>{@code auth.ldap.baseDn} - Base DN for searches
 *   <li>{@code auth.ldap.userFilter} - User search filter
 *   <li>{@code auth.ldap.jitProvisioningEnabled=true} - Enable JIT provisioning
 *   <li>{@code auth.ldap.extractGroupsEnabled=true} - Enable group extraction (optional)
 * </ul>
 *
 * @see javax.security.auth.spi.LoginModule
 * @see com.sun.security.auth.module.LdapLoginModule
 */
@Slf4j
public class LdapProvisioningLoginModule implements LoginModule {

  /** The Subject being authenticated. Populated by previous modules in the JAAS chain. */
  private Subject subject;

  /** CallbackHandler for obtaining user credentials (not used in this implementation). */
  private CallbackHandler callbackHandler;

  /**
   * Shared state between JAAS modules. Contains username and password stored by LdapLoginModule.
   * Keys: "javax.security.auth.login.name" and "javax.security.auth.login.password"
   */
  private Map<String, ?> sharedState;

  /** Configuration options passed from jaas.conf (e.g., debug="true"). */
  private Map<String, ?> options;

  /** Debug flag - enables verbose logging when set to true. */
  private boolean debug = false;

  /** Indicates whether the login() phase succeeded. */
  private boolean succeeded = false;

  /** The username extracted from Subject or sharedState. */
  private String username;

  /**
   * The user's password extracted from sharedState. Used to authenticate to LDAP for searching user
   * attributes and groups. Cleared in abort() and logout() for security.
   */
  private String userPassword;

  /** LDAP configuration loaded from application.conf. */
  private LdapConfigs ldapConfigs;

  /** Factory for creating LDAP connections. */
  private LdapConnectionFactory connectionFactory;

  /** Utility for extracting user information from LDAP. */
  private LdapUserExtractor userExtractor;

  /** Utility for extracting group information from LDAP. */
  private LdapGroupExtractor groupExtractor;

  /**
   * Initializes the LoginModule.
   *
   * <p>This method is called once when the LoginModule is instantiated. It sets up the module with
   * the provided Subject, CallbackHandler, sharedState, and options. It also loads the LDAP
   * configuration and initializes the LDAP connection components.
   *
   * <p><b>JAAS Lifecycle:</b> This is the first method called in the JAAS authentication process.
   *
   * <p><b>What it does:</b>
   *
   * <ol>
   *   <li>Stores references to Subject, CallbackHandler, sharedState, and options
   *   <li>Checks for debug option in the configuration
   *   <li>Loads LDAP configuration from application.conf
   *   <li>Initializes LdapConnectionFactory, LdapUserExtractor, and LdapGroupExtractor
   * </ol>
   *
   * @param subject The Subject to be authenticated
   * @param callbackHandler A CallbackHandler for communicating with the user (not used)
   * @param sharedState State shared with other LoginModules (contains username/password)
   * @param options Options specified in the jaas.conf configuration
   */
  @Override
  public void initialize(
      Subject subject,
      CallbackHandler callbackHandler,
      Map<String, ?> sharedState,
      Map<String, ?> options) {

    this.subject = subject;
    this.callbackHandler = callbackHandler;
    this.sharedState = sharedState;
    this.options = options;

    // Check for debug option
    this.debug = "true".equalsIgnoreCase((String) options.get("debug"));

    if (debug) {
      log.debug("LdapProvisioningLoginModule initialized");
    }

    try {
      // Load configuration
      Config config = ConfigFactory.load();
      this.ldapConfigs = new LdapConfigs(config);
      this.connectionFactory = new LdapConnectionFactory(ldapConfigs);
      this.userExtractor = new LdapUserExtractor(ldapConfigs, connectionFactory);
      this.groupExtractor = new LdapGroupExtractor(ldapConfigs, connectionFactory);

      if (debug) {
        log.debug("LDAP provisioning components initialized successfully");
      }
    } catch (Exception e) {
      log.error("Failed to initialize LDAP provisioning components", e);
    }
  }

  /**
   * Performs the authentication (Phase 1 of JAAS two-phase commit).
   *
   * <p>This method is called during the authentication phase of the JAAS login process. Since this
   * module runs AFTER LdapLoginModule (which has already authenticated the user), this method
   * extracts the password from CallbackHandler for later use in provisioning.
   *
   * <p><b>JAAS Lifecycle:</b> This is the second method called, after initialize().
   *
   * <p><b>Password Extraction:</b>
   *
   * <p>We extract the password from CallbackHandler during the login() phase because:
   *
   * <ul>
   *   <li>CallbackHandler is available and accessible during login()
   *   <li>The password is needed later in commit() for LDAP provisioning
   *   <li>sharedState is unreliable (LdapLoginModule doesn't populate it)
   * </ul>
   *
   * <p><b>Security:</b> The password is stored temporarily in memory and cleared in abort() and
   * logout() methods.
   *
   * @return true to indicate the module is ready to proceed to commit phase
   * @throws LoginException if login fails (not thrown in this implementation)
   */
  @Override
  public boolean login() throws LoginException {
    if (debug) {
      log.debug("LdapProvisioningLoginModule.login() called");
      log.debug("Subject principals at login(): {}", subject.getPrincipals());
      log.debug(
          "SharedState keys at login(): {}", sharedState != null ? sharedState.keySet() : "null");
    }

    // Extract password from CallbackHandler - REQUIRED for security
    if (callbackHandler == null) {
      log.error("CallbackHandler is null. Cannot extract password. User credentials are required.");
      return false;
    }

    userPassword = extractPasswordFromCallbackHandler();
    if (userPassword == null || userPassword.isEmpty()) {
      log.error(
          "Could not extract password from CallbackHandler. "
              + "User credentials are required for LDAP provisioning. "
              + "Ensure CallbackHandler supports PasswordCallback.");
      return false;
    }

    if (debug) {
      log.debug("Successfully extracted password from CallbackHandler");
    }

    succeeded = true;
    return true;
  }

  /**
   * Commits the authentication (Phase 2 of JAAS two-phase commit).
   *
   * <p>This method is called after all LoginModules in the chain have successfully completed their
   * login() phase. This is where the actual user and group provisioning happens.
   *
   * <p><b>JAAS Lifecycle:</b> This is the third method called, after all modules' login() methods
   * have succeeded.
   *
   * <p><b>What it does:</b>
   *
   * <ol>
   *   <li>Extracts username from Subject principals (populated by LdapLoginModule)
   *   <li>Extracts password from sharedState (stored by LdapLoginModule with storePass="true")
   *   <li>Calls {@link #provisionUserAndGroups} to provision user and groups in DataHub
   *   <li>Adds DataHubUserPrincipal to the Subject for use by the application
   * </ol>
   *
   * <p><b>Security Note:</b> The password is extracted from sharedState and used to authenticate to
   * LDAP for searching user attributes and groups. This eliminates the need to store service
   * account credentials in configuration. The password is only held in memory during the login
   * session and is cleared in abort() and logout().
   *
   * <p><b>Error Handling:</b> If provisioning fails, a LoginException is thrown, which causes the
   * entire JAAS authentication to fail. This ensures users cannot log in if they cannot be properly
   * provisioned in DataHub.
   *
   * @return true if commit succeeds, false if login() did not succeed
   * @throws LoginException if username cannot be extracted or provisioning fails
   */
  @Override
  public boolean commit() throws LoginException {
    if (!succeeded) {
      return false;
    }

    if (debug) {
      log.debug("LdapProvisioningLoginModule.commit() called");
      log.debug("Subject principals at commit(): {}", subject.getPrincipals());
      log.debug(
          "SharedState keys at commit(): {}", sharedState != null ? sharedState.keySet() : "null");
    }

    // NOW extract username - Subject should be populated by LdapLoginModule.commit()
    username = extractUsernameFromSubject();

    // Fallback: try sharedState if Subject is still empty
    if (username == null || username.isEmpty()) {
      if (debug) {
        log.debug("Username not found in Subject, trying sharedState...");
      }
      username = extractUsernameFromSharedState();
    }

    if (username == null || username.isEmpty()) {
      log.error(
          "No username found in commit phase. Subject principals: {}, SharedState keys: {}",
          subject.getPrincipals(),
          sharedState != null ? sharedState.keySet() : "null");
      throw new LoginException("No username found for provisioning");
    }

    // Password should have been extracted in login() phase from CallbackHandler
    if (userPassword == null || userPassword.isEmpty()) {
      log.warn(
          "User password not available. Will fall back to configured bindDn/bindPassword if available.");
    } else if (debug) {
      log.debug("User password is available for LDAP provisioning");
    }

    if (debug) {
      log.debug("Provisioning user: {}", username);
    }

    try {
      // Perform user and group provisioning using user's credentials
      provisionUserAndGroups(username, userPassword);

      // Add DataHubUserPrincipal to subject if not already present
      boolean principalExists =
          subject.getPrincipals().stream()
              .anyMatch(
                  p ->
                      p instanceof DataHubUserPrincipal
                          && ((DataHubUserPrincipal) p).getName().equals(username));

      if (!principalExists) {
        subject.getPrincipals().add(new DataHubUserPrincipal(username));
        if (debug) {
          log.debug("Added DataHubUserPrincipal for user: {}", username);
        }
      }

      log.info("Successfully provisioned user and groups for: {}", username);
      return true;

    } catch (Exception e) {
      log.error("Failed to provision user and groups for: {}", username, e);
      throw new LoginException("Failed to provision user: " + e.getMessage());
    }
  }

  /**
   * Aborts the authentication process.
   *
   * <p>This method is called if the overall authentication fails (i.e., if any LoginModule in the
   * chain fails). It cleans up any state that was set during the login() phase.
   *
   * <p><b>JAAS Lifecycle:</b> This method is called instead of commit() if authentication fails.
   *
   * <p><b>What it does:</b>
   *
   * <ol>
   *   <li>Resets the succeeded flag to false
   *   <li>Clears the username
   *   <li>Clears the password (for security)
   * </ol>
   *
   * <p><b>Security Note:</b> It's important to clear the password here to ensure it doesn't remain
   * in memory after a failed login attempt.
   *
   * @return true to indicate abort succeeded
   * @throws LoginException if abort fails (not thrown in this implementation)
   */
  @Override
  public boolean abort() throws LoginException {
    if (debug) {
      log.debug("LdapProvisioningLoginModule.abort() called");
    }

    succeeded = false;
    username = null;
    userPassword = null; // Clear password for security
    return true;
  }

  /**
   * Logs out the user.
   *
   * <p>This method is called when the user logs out or when the session expires. It removes the
   * principals that were added during commit() and cleans up any sensitive state.
   *
   * <p><b>JAAS Lifecycle:</b> This method is called when the application explicitly logs out the
   * user.
   *
   * <p><b>What it does:</b>
   *
   * <ol>
   *   <li>Removes DataHubUserPrincipal from the Subject
   *   <li>Resets the succeeded flag
   *   <li>Clears the username
   *   <li>Clears the password (for security)
   * </ol>
   *
   * <p><b>Security Note:</b> Clearing the password ensures it doesn't remain in memory after
   * logout.
   *
   * @return true to indicate logout succeeded
   * @throws LoginException if logout fails (not thrown in this implementation)
   */
  @Override
  public boolean logout() throws LoginException {
    if (debug) {
      log.debug("LdapProvisioningLoginModule.logout() called");
    }

    // Remove DataHubUserPrincipal from subject
    subject.getPrincipals().removeIf(p -> p instanceof DataHubUserPrincipal);

    succeeded = false;
    username = null;
    userPassword = null; // Clear password for security
    return true;
  }

  /**
   * Extracts the username from the Subject's principals.
   *
   * <p>The username should have been set by the LdapLoginModule during its commit() phase.
   * LdapLoginModule typically adds two principals to the Subject:
   *
   * <ol>
   *   <li><b>LdapPrincipal:</b> Contains the full Distinguished Name (DN), e.g., "CN=John
   *       Doe,OU=Users,DC=example,DC=com"
   *   <li><b>UserPrincipal:</b> Contains the simple username, e.g., "jdoe"
   * </ol>
   *
   * <p>This method prefers UserPrincipal as it contains the actual username that should be used for
   * provisioning in DataHub.
   *
   * <p><b>Two-pass approach:</b>
   *
   * <ol>
   *   <li><b>First pass:</b> Look specifically for UserPrincipal (preferred)
   *   <li><b>Second pass:</b> Try to extract username from other principal types (fallback)
   * </ol>
   *
   * @return The username, or null if not found
   */
  private String extractUsernameFromSubject() {
    Set<Principal> principals = subject.getPrincipals();

    if (principals.isEmpty()) {
      if (debug) {
        log.debug("Subject has no principals");
      }
      return null;
    }

    // First pass: Look for UserPrincipal (preferred - contains simple username)
    for (Principal principal : principals) {
      String principalName = principal.getName();
      String principalType = principal.getClass().getName();

      if (debug) {
        log.debug("Examining principal: {} (type: {})", principalName, principalType);
      }

      // UserPrincipal contains the simple username (e.g., "jdoe")
      if (principalType.contains("UserPrincipal")) {
        if (debug) {
          log.debug("Found UserPrincipal with username: {}", principalName);
        }
        return principalName;
      }
    }

    // Second pass: Try to extract from other principal types
    for (Principal principal : principals) {
      String principalName = principal.getName();
      String username = extractUsernameFromPrincipal(principalName);

      if (username != null && !username.isEmpty()) {
        if (debug) {
          log.debug("Extracted username from principal: {}", username);
        }
        return username;
      }
    }

    if (debug) {
      log.debug("Could not extract username from any principal");
    }
    return null;
  }

  /**
   * Extracts username from sharedState.
   *
   * <p>This is a fallback mechanism if the Subject is not yet populated. The sharedState is
   * populated by previous LoginModules in the JAAS chain (e.g., LdapLoginModule) during their
   * login() phase.
   *
   * <p><b>Standard JAAS keys tried:</b>
   *
   * <ol>
   *   <li>{@code javax.security.auth.login.name} - Standard JAAS username key
   *   <li>{@code java.naming.security.principal} - LDAP-specific principal key
   * </ol>
   *
   * @return The username from sharedState, or null if not found
   */
  private String extractUsernameFromSharedState() {
    if (sharedState == null || sharedState.isEmpty()) {
      if (debug) {
        log.debug("SharedState is null or empty");
      }
      return null;
    }

    // Try standard JAAS key for username
    Object usernameObj = sharedState.get("javax.security.auth.login.name");
    if (usernameObj != null) {
      String username = usernameObj.toString();
      if (debug) {
        log.debug("Found username in sharedState (javax.security.auth.login.name): {}", username);
      }
      return extractUsernameFromPrincipal(username);
    }

    // Try LDAP-specific key
    usernameObj = sharedState.get("java.naming.security.principal");
    if (usernameObj != null) {
      String username = usernameObj.toString();
      if (debug) {
        log.debug("Found username in sharedState (java.naming.security.principal): {}", username);
      }
      return extractUsernameFromPrincipal(username);
    }

    if (debug) {
      log.debug("No username found in sharedState. Available keys: {}", sharedState.keySet());
    }
    return null;
  }

  /**
   * Extracts the user's password from CallbackHandler.
   *
   * <p><b>This is the primary method</b> for obtaining the password as it directly retrieves it
   * from the authentication source, bypassing the unreliable sharedState mechanism.
   *
   * <p><b>How it works:</b>
   *
   * <ol>
   *   <li>Creates a PasswordCallback to request the password
   *   <li>Invokes the CallbackHandler with the callback
   *   <li>Retrieves the password from the callback
   *   <li>Clears the callback's password for security
   * </ol>
   *
   * <p><b>Why this approach:</b>
   *
   * <ul>
   *   <li><b>Reliable:</b> Doesn't depend on previous modules storing password in sharedState
   *   <li><b>Direct:</b> Gets password from the original authentication source
   *   <li><b>No configuration:</b> Doesn't require storePass="true" in jaas.conf
   *   <li><b>Secure:</b> Uses the user's own credentials for LDAP operations
   * </ul>
   *
   * <p><b>Why we need the password:</b> We use the user's password to authenticate to LDAP for
   * searching user attributes and groups. This is more secure than storing service account
   * credentials in configuration, as it follows the principle of least privilege - each user can
   * only access their own LDAP data.
   *
   * <p><b>Security Note:</b> The password is only held in memory during the login session and is
   * cleared in abort() and logout() methods.
   *
   * @return The user's password as a String, or null if not available
   */
  private String extractPasswordFromCallbackHandler() {
    if (callbackHandler == null) {
      if (debug) {
        log.debug("CallbackHandler is null, cannot extract password");
      }
      return null;
    }

    try {
      // Create a PasswordCallback to retrieve the password
      PasswordCallback passwordCallback = new PasswordCallback("Password: ", false);

      // Invoke the callback handler
      callbackHandler.handle(new Callback[] {passwordCallback});

      // Get the password
      char[] password = passwordCallback.getPassword();

      // Clear the callback for security
      passwordCallback.clearPassword();

      if (password != null && password.length > 0) {
        String passwordStr = new String(password);
        // Clear the char array for security
        java.util.Arrays.fill(password, ' ');

        if (debug) {
          log.debug(
              "Successfully extracted password from CallbackHandler (length: {})",
              passwordStr.length());
        }

        return passwordStr;
      } else {
        if (debug) {
          log.debug("Password from CallbackHandler is null or empty");
        }
      }

    } catch (IOException e) {
      log.warn("IOException while extracting password from CallbackHandler: {}", e.getMessage());
      if (debug) {
        log.debug("IOException details", e);
      }
    } catch (UnsupportedCallbackException e) {
      log.warn(
          "UnsupportedCallbackException while extracting password from CallbackHandler: {}",
          e.getMessage());
      if (debug) {
        log.debug("UnsupportedCallbackException details", e);
      }
    }

    return null;
  }

  /**
   * Extracts the username from a principal name string.
   *
   * <p>Handles various principal name formats:
   *
   * <ul>
   *   <li><b>Simple username:</b> "jdoe" → "jdoe"
   *   <li><b>UPN format:</b> "jdoe@example.com" → "jdoe"
   *   <li><b>DN format:</b> "CN=John Doe,OU=Users,DC=example,DC=com" → null (skip, look for
   *       UserPrincipal instead)
   * </ul>
   *
   * <p><b>Why skip DN format?</b> Distinguished Names can contain escaped commas (e.g., "CN=Doe\,
   * John") which makes parsing unreliable. It's better to look for UserPrincipal which contains the
   * simple username.
   *
   * @param principalName The principal name to parse
   * @return The extracted username, or null if the format should be skipped
   */
  private String extractUsernameFromPrincipal(String principalName) {
    if (principalName == null || principalName.isEmpty()) {
      return null;
    }

    // If it's a UPN (user@domain.com), extract the username part
    if (principalName.contains("@")) {
      return principalName.substring(0, principalName.indexOf("@"));
    }

    // If it's a DN (CN=...), it's likely a full distinguished name
    // Don't try to parse it - we should look for UserPrincipal instead
    if (principalName.startsWith("CN=")
        || principalName.contains(",OU=")
        || principalName.contains(",DC=")) {
      if (debug) {
        log.debug("Skipping DN format principal: {}", principalName);
      }
      return null; // Signal to try next principal
    }

    // Otherwise, return as-is (simple username)
    return principalName;
  }

  /**
   * Provisions the user and their groups in DataHub using the user's own credentials.
   *
   * <p>This method orchestrates the entire provisioning process:
   *
   * <ol>
   *   <li>Gets the LdapProvisioningService singleton instance
   *   <li>If JIT provisioning is enabled:
   *       <ul>
   *         <li>Extracts user information from LDAP using user's credentials
   *         <li>Provisions the user in DataHub
   *         <li>If group extraction is enabled:
   *             <ul>
   *               <li>Gets the user's DN from LDAP
   *               <li>Extracts groups the user belongs to
   *               <li>Provisions the groups in DataHub
   *               <li>Updates the user's group memberships
   *             </ul>
   *       </ul>
   *   <li>If pre-provisioning is required:
   *       <ul>
   *         <li>Verifies that the user has been pre-provisioned in DataHub
   *       </ul>
   *   <li>Sets the user's status to ACTIVE
   * </ol>
   *
   * <p><b>Security:</b> This method uses the user's own password (passed as parameter) to
   * authenticate to LDAP for all searches. This means:
   *
   * <ul>
   *   <li>No service account credentials need to be stored in configuration
   *   <li>Each user can only access their own LDAP data (based on LDAP ACLs)
   *   <li>Follows the principle of least privilege
   * </ul>
   *
   * <p><b>Fallback:</b> If userPassword is null or empty, the LDAP extractors will fall back to
   * using configured bindDn/bindPassword if available.
   *
   * @param username The username to provision (e.g., "jdoe")
   * @param userPassword The user's password (used for LDAP authentication), or null to use
   *     configured bindDn/bindPassword
   * @throws Exception if provisioning fails for any reason
   */
  private void provisionUserAndGroups(String username, String userPassword) throws Exception {
    // Get the provisioning service
    LdapProvisioningService provisioningService;
    try {
      provisioningService = LdapProvisioningService.getInstance();
    } catch (IllegalStateException e) {
      log.error("LdapProvisioningService not initialized. Cannot provision user.");
      throw new Exception("LDAP provisioning service not available", e);
    }

    final CorpuserUrn corpUserUrn = new CorpuserUrn(username);

    // Check if JIT provisioning is enabled
    if (ldapConfigs.isJitProvisioningEnabled()) {
      log.debug("JIT provisioning is enabled. Extracting user from LDAP...");

      // Extract user information from LDAP using user's own credentials
      CorpUserSnapshot userSnapshot = userExtractor.extractUser(username, userPassword);

      // Provision the user
      provisioningService.tryProvisionUser(userSnapshot);

      // Extract and provision groups if enabled
      if (ldapConfigs.isExtractGroupsEnabled()) {
        log.debug("Group extraction is enabled. Extracting groups from LDAP...");

        // Get user DN for group search using user's credentials
        String userDn = userExtractor.getUserDn(username, userPassword);

        // Extract groups using user's credentials
        // Pass username separately (not extracted from DN) for authentication
        List<CorpGroupSnapshot> groups =
            groupExtractor.extractGroups(username, userDn, userPassword);

        if (!groups.isEmpty()) {
          // Provision groups
          provisioningService.tryProvisionGroups(groups);

          // Update user's group membership using the new signature
          provisioningService.updateGroupMembership(
              username, corpUserUrn, createGroupMembership(groups));

          log.debug("Provisioned {} groups for user {}", groups.size(), username);
        } else {
          log.debug("No groups found for user {}", username);
        }
      }

    } else if (ldapConfigs.isPreProvisioningRequired()) {
      log.debug("Pre-provisioning is required. Verifying user exists...");

      // Verify that the user has been pre-provisioned
      provisioningService.verifyPreProvisionedUser(corpUserUrn);
    }

    // Set user status to active
    CorpUserStatus activeStatus =
        new CorpUserStatus()
            .setStatus(Constants.CORP_USER_STATUS_ACTIVE)
            .setLastModified(
                new AuditStamp()
                    .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
                    .setTime(System.currentTimeMillis()));

    provisioningService.setUserStatus(username, corpUserUrn, activeStatus);

    log.debug("User provisioning completed for: {}", username);
  }

  /**
   * Creates a GroupMembership aspect from a list of CorpGroupSnapshots.
   *
   * <p>This helper method extracts the URNs from the group snapshots and creates a GroupMembership
   * aspect that can be used to update a user's group memberships in DataHub.
   *
   * <p><b>Pattern:</b> This follows the same pattern as OidcCallbackLogic.createGroupMembership(),
   * ensuring consistency across authentication methods.
   *
   * @param extractedGroups List of group snapshots extracted from LDAP
   * @return GroupMembership aspect containing the group URNs
   */
  private GroupMembership createGroupMembership(final List<CorpGroupSnapshot> extractedGroups) {
    final GroupMembership groupMembershipAspect = new GroupMembership();
    groupMembershipAspect.setGroups(
        new UrnArray(
            extractedGroups.stream().map(CorpGroupSnapshot::getUrn).collect(Collectors.toList())));
    return groupMembershipAspect;
  }
}
