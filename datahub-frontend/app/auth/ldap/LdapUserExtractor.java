package auth.ldap;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.SetMode;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.aspect.CorpUserAspectArray;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class to extract user information from LDAP and convert it to DataHub CorpUserSnapshot.
 *
 * <p>This class provides methods to:
 *
 * <ul>
 *   <li><b>Extract user attributes:</b> Retrieves firstName, lastName, email, displayName from LDAP
 *   <li><b>Get user DN:</b> Retrieves the full Distinguished Name for group membership searches
 *   <li><b>Secure authentication:</b> Supports using user's own credentials (preferred) or service
 *       account
 * </ul>
 *
 * <p><b>Security Model:</b>
 *
 * <p>This class supports two authentication modes:
 *
 * <ol>
 *   <li><b>User Credentials (Preferred):</b> Uses the user's password to authenticate to LDAP. This
 *       is more secure as it doesn't require storing service account credentials and follows the
 *       principle of least privilege.
 *   <li><b>Service Account (Fallback):</b> Uses configured bindDn/bindPassword. Required for batch
 *       operations or when user credentials are not available.
 * </ol>
 *
 * <p><b>Usage Examples:</b>
 *
 * <pre>
 * LdapUserExtractor extractor = new LdapUserExtractor(configs, connectionFactory);
 *
 * // Preferred: Using user's own credentials
 * CorpUserSnapshot user = extractor.extractUser("jdoe", "userPassword");
 * String userDn = extractor.getUserDn("jdoe", "userPassword");
 *
 * // Fallback: Using service account (requires bindDn/bindPassword configured)
 * CorpUserSnapshot user = extractor.extractUser("jdoe");
 * String userDn = extractor.getUserDn("jdoe");
 * </pre>
 *
 * <p><b>LDAP Attributes Extracted:</b>
 *
 * <ul>
 *   <li>{@code userPrincipalName} or {@code uid} - User identifier
 *   <li>{@code givenName} - First name
 *   <li>{@code sn} - Last name (surname)
 *   <li>{@code mail} - Email address
 *   <li>{@code displayName} - Display name
 *   <li>{@code distinguishedName} - Full DN
 * </ul>
 *
 * <p><b>Configuration:</b> Reads from {@link LdapConfigs}:
 *
 * <ul>
 *   <li>{@code auth.ldap.server} - LDAP server URL
 *   <li>{@code auth.ldap.baseDn} - Base DN for searches
 *   <li>{@code auth.ldap.userFilter} - User search filter (e.g.,
 *       "(userPrincipalName={0}@example.com)")
 *   <li>{@code auth.ldap.userIdAttribute} - Attribute containing username (default: "uid")
 *   <li>{@code auth.ldap.bindDn} - Service account DN (optional)
 *   <li>{@code auth.ldap.bindPassword} - Service account password (optional)
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. Multiple threads can safely use the same
 * instance.
 *
 * @see LdapConfigs
 * @see LdapConnectionFactory
 * @see com.linkedin.metadata.snapshot.CorpUserSnapshot
 */
@Slf4j
public class LdapUserExtractor {

  private final LdapConfigs configs;
  private final LdapConnectionFactory connectionFactory;
  private final boolean debug;

  public LdapUserExtractor(
      @Nonnull LdapConfigs configs, @Nonnull LdapConnectionFactory connectionFactory) {
    this.configs = configs;
    this.connectionFactory = connectionFactory;
    this.debug = log.isDebugEnabled();
  }

  /**
   * Extracts user information from LDAP using the user's own credentials for authentication.
   *
   * <p><b>This method enforces security best practices</b> by requiring user credentials and not
   * falling back to service account credentials. This ensures:
   *
   * <ul>
   *   <li>No service account credentials need to be stored in configuration
   *   <li>Each user can only access their own LDAP attributes (based on LDAP ACLs)
   *   <li>Password is only held in memory during the login session
   *   <li>Follows principle of least privilege
   * </ul>
   *
   * <p><b>How it works:</b>
   *
   * <ol>
   *   <li>User logs in with username "jdoe" and password "userpass123"
   *   <li>LdapLoginModule authenticates the user against LDAP
   *   <li>LdapProvisioningLoginModule extracts the password from CallbackHandler
   *   <li>This method uses "jdoe" + "userpass123" to bind to LDAP
   *   <li>Searches for user's attributes (firstName, lastName, email, etc.)
   *   <li>Returns CorpUserSnapshot with the extracted information
   * </ol>
   *
   * @param username The username to search for (e.g., "jdoe")
   * @param userPassword The user's password obtained during login (REQUIRED - used to authenticate
   *     to LDAP)
   * @return CorpUserSnapshot containing user information extracted from LDAP attributes
   * @throws Exception if user cannot be found, extracted, LDAP connection fails, or if userPassword
   *     is null/empty
   */
  public CorpUserSnapshot extractUser(@Nonnull String username, @Nonnull String userPassword)
      throws Exception {

    if (userPassword == null || userPassword.isEmpty()) {
      throw new IllegalArgumentException(
          "User password is required for LDAP operations. "
              + "Storing bindDn/bindPassword in configuration is not supported for security reasons.");
    }

    DirContext ctx = null;
    try {
      // Build user's DN or UPN for authentication (e.g., "jdoe@corpdev.example.com")
      String userDn = buildUserDnForAuth(username);
      ctx = connectionFactory.createConnection(userDn, userPassword);
      log.debug("Using user's own credentials for LDAP search");

      return extractUserWithContext(ctx, username);

    } catch (NamingException e) {
      log.error("Failed to extract user from LDAP: {}", e.getMessage(), e);
      throw new Exception("Failed to extract user from LDAP: " + e.getMessage(), e);
    } finally {
      connectionFactory.closeConnection(ctx);
    }
  }

  /**
   * Builds the user's DN (Distinguished Name) or UPN (User Principal Name) for LDAP authentication.
   *
   * <p>This method constructs the authentication identity that will be used to bind to LDAP. The
   * format depends on your LDAP configuration:
   *
   * <p><b>Examples:</b>
   *
   * <ul>
   *   <li>If userFilter contains "@example.com", returns "jdoe@example.com" (UPN format)
   *   <li>If userFilter is simple, returns "jdoe" (simple username)
   *   <li>Could be extended to return full DN like "CN=jdoe,OU=Users,DC=example,DC=com"
   * </ul>
   *
   * <p><b>Configuration example:</b><br>
   * If auth.ldap.userFilter = "(userPrincipalName={0}@example.com)"<br>
   * Then this method extracts "@example.com" and returns "jdoe@example.com"
   *
   * @param username The simple username (e.g., "jdoe")
   * @return The authentication identity to use for LDAP bind (e.g., "jdoe@example.com")
   */
  private String buildUserDnForAuth(String username) {
    // Check if userFilter contains UPN pattern (e.g., {0}@domain.com or
    // userPrincipalName={0}@domain.com)
    String userFilter = configs.getUserFilter();

    if (debug) {
      log.debug("Building auth identity from userFilter: {}", userFilter);
    }

    // If filter contains @, extract the domain and build UPN
    if (userFilter.contains("@")) {
      int atIndex = userFilter.indexOf("@");
      // Find the end of the domain - could be ), space, or end of string
      int endIndex = userFilter.length();

      // Look for closing parenthesis
      int parenIndex = userFilter.indexOf(")", atIndex);
      if (parenIndex > atIndex) {
        endIndex = parenIndex;
      }

      // Look for space (in case of multiple conditions)
      int spaceIndex = userFilter.indexOf(" ", atIndex);
      if (spaceIndex > atIndex && spaceIndex < endIndex) {
        endIndex = spaceIndex;
      }

      if (endIndex > atIndex + 1) {
        String domain = userFilter.substring(atIndex + 1, endIndex);
        String upn = username + "@" + domain;
        if (debug) {
          log.debug("Built UPN for authentication: {}", upn);
        }
        return upn;
      }
    }

    // Otherwise, just return username (will work for simple bind)
    if (debug) {
      log.debug("Using simple username for authentication: {}", username);
    }
    return username;
  }

  /**
   * Internal helper method that performs the actual LDAP search using an existing directory
   * context.
   *
   * <p>This method is called by both extractUser() methods after they've established an LDAP
   * connection (either with service account credentials or user's own credentials).
   *
   * <p><b>What it does:</b>
   *
   * <ol>
   *   <li>Configures search parameters (scope, attributes to retrieve)
   *   <li>Builds search filter from configured userFilter (e.g., "(userPrincipalName=jdoe@...)")
   *   <li>Executes LDAP search in the configured baseDn
   *   <li>Extracts LDAP attributes (givenName, sn, mail, displayName, etc.)
   *   <li>Converts LDAP attributes to DataHub CorpUserSnapshot format
   * </ol>
   *
   * <p><b>Note:</b> This method does NOT close the context - that's the responsibility of the
   * caller.
   *
   * @param ctx The LDAP directory context (already authenticated)
   * @param username The username to search for
   * @return CorpUserSnapshot containing user information
   * @throws Exception if user cannot be found or if LDAP search fails
   */
  private CorpUserSnapshot extractUserWithContext(DirContext ctx, String username)
      throws Exception {
    try {

      // Search for the user
      SearchControls searchControls = new SearchControls();
      searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

      // Set attributes to retrieve
      String[] attributesToReturn = {
        configs.getUserIdAttribute(),
        configs.getUserFirstNameAttribute(),
        configs.getUserLastNameAttribute(),
        configs.getUserEmailAttribute(),
        configs.getUserDisplayNameAttribute(),
        "cn",
        "distinguishedName",
        "userPrincipalName",
        "mail",
        "displayName",
        "givenName",
        "sn"
      };
      searchControls.setReturningAttributes(attributesToReturn);

      // Build search filter
      String searchFilter = configs.getUserFilter().replace("{0}", username);

      log.debug(
          "Searching for user with filter: {} in base DN: {}", searchFilter, configs.getBaseDn());

      NamingEnumeration<SearchResult> results =
          ctx.search(configs.getBaseDn(), searchFilter, searchControls);

      if (!results.hasMore()) {
        throw new Exception("User not found in LDAP: " + username);
      }

      SearchResult searchResult = results.next();
      Attributes attributes = searchResult.getAttributes();

      log.debug("Found user in LDAP: {}", searchResult.getNameInNamespace());

      // Extract attributes into a map
      Map<String, String> attributeMap = extractAttributesToMap(attributes);

      // Build and return CorpUserSnapshot
      CorpUserSnapshot userSnapshot = buildCorpUserSnapshot(username, attributeMap);

      log.debug("Successfully extracted user information for: {}", username);

      return userSnapshot;

    } catch (NamingException e) {
      log.error("Failed to extract user attributes: {}", e.getMessage());
      throw new Exception("Failed to extract user attributes: " + e.getMessage(), e);
    }
  }

  /**
   * Extracts LDAP attributes into a Map for easier processing.
   *
   * @param attributes The LDAP attributes
   * @return Map of attribute names to values
   */
  private Map<String, String> extractAttributesToMap(Attributes attributes) {
    Map<String, String> attributeMap = new HashMap<>();

    try {
      NamingEnumeration<? extends Attribute> allAttributes = attributes.getAll();
      while (allAttributes.hasMore()) {
        Attribute attribute = allAttributes.next();
        String attributeName = attribute.getID();
        Object attributeValue = attribute.get();

        if (attributeValue != null) {
          attributeMap.put(attributeName, attributeValue.toString());
        }
      }
    } catch (NamingException e) {
      log.warn("Error extracting attributes to map: {}", e.getMessage());
    }

    return attributeMap;
  }

  /**
   * Builds a CorpUserSnapshot from extracted LDAP attributes.
   *
   * @param username The username
   * @param attributes Map of LDAP attributes
   * @return CorpUserSnapshot
   */
  private CorpUserSnapshot buildCorpUserSnapshot(String username, Map<String, String> attributes) {
    final CorpUserSnapshot userSnapshot = new CorpUserSnapshot();
    userSnapshot.setUrn(new CorpuserUrn(username));

    final CorpUserAspectArray aspects = new CorpUserAspectArray();

    // Build CorpUserInfo aspect
    final CorpUserInfo userInfo = new CorpUserInfo();
    userInfo.setActive(true);

    // Extract name information
    String displayName = attributes.getOrDefault("displayName", username);
    String givenName = attributes.getOrDefault("givenName", "");
    String surname = attributes.getOrDefault("sn", "");
    String fullName = displayName;
    if (fullName.equals(username) && !givenName.isEmpty() && !surname.isEmpty()) {
      fullName = givenName + " " + surname;
    }

    userInfo.setFullName(fullName, SetMode.IGNORE_NULL);
    userInfo.setDisplayName(displayName, SetMode.IGNORE_NULL);

    // Set email
    String email =
        attributes.getOrDefault("mail", attributes.getOrDefault("userPrincipalName", ""));
    if (!email.isEmpty()) {
      userInfo.setEmail(email, SetMode.IGNORE_NULL);
    }

    // Add the aspect
    aspects.add(CorpUserAspect.create(userInfo));

    // Build the key
    final CorpUserKey userKey = new CorpUserKey();
    userKey.setUsername(username);
    aspects.add(CorpUserAspect.create(userKey));

    userSnapshot.setAspects(aspects);
    return userSnapshot;
  }

  /**
   * Gets the Distinguished Name (DN) of a user using the user's own credentials.
   *
   * <p><b>This method enforces security best practices</b> by requiring user credentials. The DN is
   * the full LDAP path to the user object, for example: "CN=John
   * Doe,OU=Users,OU=Engineering,DC=example,DC=com"
   *
   * <p>This DN is needed for group membership searches, as LDAP groups typically store member DNs
   * rather than simple usernames.
   *
   * <p><b>How it works:</b>
   *
   * <ol>
   *   <li>Authenticates to LDAP using username + password
   *   <li>Searches for the user's LDAP entry
   *   <li>Returns the full DN from the search result
   * </ol>
   *
   * @param username The username to search for (e.g., "jdoe")
   * @param userPassword The user's password (REQUIRED - used for LDAP authentication)
   * @return The user's full Distinguished Name in LDAP
   * @throws Exception if user cannot be found, LDAP connection fails, or if userPassword is
   *     null/empty
   */
  public String getUserDn(@Nonnull String username, @Nonnull String userPassword) throws Exception {

    if (userPassword == null || userPassword.isEmpty()) {
      throw new IllegalArgumentException(
          "User password is required for LDAP operations. "
              + "Storing bindDn/bindPassword in configuration is not supported for security reasons.");
    }

    DirContext ctx = null;
    try {
      String userDn = buildUserDnForAuth(username);
      ctx = connectionFactory.createConnection(userDn, userPassword);
      log.debug("Using user's own credentials to get DN");

      return getUserDnWithContext(ctx, username);
    } catch (NamingException e) {
      log.error("Failed to get user DN from LDAP: {}", e.getMessage(), e);
      throw new Exception("Failed to get user DN from LDAP: " + e.getMessage(), e);
    } finally {
      connectionFactory.closeConnection(ctx);
    }
  }

  /**
   * Internal helper method that retrieves the user's DN using an existing directory context.
   *
   * <p>This method performs an LDAP search to find the user and returns their full Distinguished
   * Name from the search result.
   *
   * <p><b>Note:</b> This method does NOT close the context - that's the responsibility of the
   * caller.
   *
   * @param ctx The LDAP directory context (already authenticated)
   * @param username The username to search for
   * @return The user's full Distinguished Name
   * @throws Exception if user cannot be found or if LDAP search fails
   */
  private String getUserDnWithContext(DirContext ctx, String username) throws Exception {
    try {
      SearchControls searchControls = new SearchControls();
      searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
      searchControls.setReturningAttributes(new String[] {"distinguishedName"});

      String searchFilter = configs.getUserFilter().replace("{0}", username);

      NamingEnumeration<SearchResult> results =
          ctx.search(configs.getBaseDn(), searchFilter, searchControls);

      if (!results.hasMore()) {
        throw new Exception("User not found in LDAP: " + username);
      }

      SearchResult searchResult = results.next();
      return searchResult.getNameInNamespace();

    } catch (NamingException e) {
      log.error("Failed to get user DN: {}", e.getMessage());
      throw new Exception("Failed to get user DN: " + e.getMessage(), e);
    }
  }
}
