package auth.ldap;

import com.linkedin.common.CorpGroupUrnArray;
import com.linkedin.common.CorpuserUrnArray;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.aspect.CorpGroupAspectArray;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
 * Utility class to extract group information from LDAP and convert it to DataHub CorpGroupSnapshot.
 *
 * <p>This class provides methods to:
 *
 * <ul>
 *   <li><b>Extract group memberships:</b> Finds all groups a user belongs to
 *   <li><b>Extract group details:</b> Retrieves group name, description, and other attributes
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
 *       principle of least privilege - users can only see their own group memberships.
 *   <li><b>Service Account (Fallback):</b> Uses configured bindDn/bindPassword. Required for batch
 *       operations or when user credentials are not available.
 * </ol>
 *
 * <p><b>Usage Examples:</b>
 *
 * <pre>
 * LdapGroupExtractor extractor = new LdapGroupExtractor(configs, connectionFactory);
 *
 * // Get user's DN first (needed for group search)
 * String userDn = "CN=John Doe,OU=Users,DC=example,DC=com";
 *
 * // Preferred: Using user's own credentials
 * List&lt;CorpGroupSnapshot&gt; groups = extractor.extractGroups(userDn, "userPassword");
 *
 * // Fallback: Using service account (requires bindDn/bindPassword configured)
 * List&lt;CorpGroupSnapshot&gt; groups = extractor.extractGroups(userDn);
 *
 * // Lightweight: Get just group names
 * List&lt;String&gt; groupNames = extractor.extractGroupNames(userDn);
 * </pre>
 *
 * <p><b>How Group Search Works:</b>
 *
 * <ol>
 *   <li>Takes the user's full DN (e.g., "CN=John Doe,OU=Users,DC=example,DC=com")
 *   <li>Searches for groups where the user's DN appears in the member attribute
 *   <li>Uses configured groupFilter (e.g., "(member={0})" where {0} is replaced with user DN)
 *   <li>Extracts group attributes (name, description, etc.)
 *   <li>Converts to DataHub CorpGroupSnapshot format
 * </ol>
 *
 * <p><b>LDAP Group Attributes Extracted:</b>
 *
 * <ul>
 *   <li>{@code cn} - Common name (group name)
 *   <li>{@code description} - Group description
 *   <li>{@code distinguishedName} - Full DN of the group
 *   <li>Custom attributes via {@code groupNameAttribute} configuration
 * </ul>
 *
 * <p><b>Configuration:</b> Reads from {@link LdapConfigs}:
 *
 * <ul>
 *   <li>{@code auth.ldap.extractGroupsEnabled} - Enable/disable group extraction (default: false)
 *   <li>{@code auth.ldap.groupBaseDn} - Base DN for group searches (e.g., "DC=example,DC=com")
 *   <li>{@code auth.ldap.groupFilter} - Group search filter (e.g., "(member={0})")
 *   <li>{@code auth.ldap.groupNameAttribute} - Attribute containing group name (default: "cn")
 *   <li>{@code auth.ldap.bindDn} - Service account DN (optional)
 *   <li>{@code auth.ldap.bindPassword} - Service account password (optional)
 * </ul>
 *
 * <p><b>Common Group Filters:</b>
 *
 * <ul>
 *   <li><b>Active Directory:</b> {@code (member={0})} - Direct membership
 *   <li><b>Active Directory (nested):</b> {@code (member:1.2.840.113556.1.4.1941:={0})} - Includes
 *       nested groups
 *   <li><b>OpenLDAP:</b> {@code (memberUid={0})} - Uses memberUid attribute
 *   <li><b>Custom:</b> {@code (&(objectClass=group)(member={0}))} - With object class filter
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. Multiple threads can safely use the same
 * instance.
 *
 * <p><b>Performance Note:</b> Group extraction can be slow for users with many group memberships.
 * Consider using {@link #extractGroupNames(String)} if you only need group names without full
 * details.
 *
 * @see LdapConfigs
 * @see LdapConnectionFactory
 * @see com.linkedin.metadata.snapshot.CorpGroupSnapshot
 */
@Slf4j
public class LdapGroupExtractor {

  private final LdapConfigs configs;
  private final LdapConnectionFactory connectionFactory;

  public LdapGroupExtractor(
      @Nonnull LdapConfigs configs, @Nonnull LdapConnectionFactory connectionFactory) {
    this.configs = configs;
    this.connectionFactory = connectionFactory;
  }

  /**
   * Extracts groups for a user from LDAP using the user's own credentials.
   *
   * <p><b>This is the preferred method for security reasons</b> as it doesn't require storing
   * service account credentials. Instead, it uses the password that the user just provided during
   * login to authenticate to LDAP and search for their group memberships.
   *
   * <p><b>How it works:</b>
   *
   * <ol>
   *   <li>User logs in with username "jdoe" and password "userpass123"
   *   <li>LdapLoginModule authenticates the user against LDAP
   *   <li>LdapProvisioningLoginModule extracts the password from CallbackHandler
   *   <li>This method uses "jdoe" + "userpass123" to bind to LDAP
   *   <li>Searches for groups where the user's DN appears in the member attribute
   *   <li>Returns list of CorpGroupSnapshot objects
   * </ol>
   *
   * <p><b>Security Benefits:</b>
   *
   * <ul>
   *   <li>No need to store service account credentials in configuration
   *   <li>Each user can only access their own group memberships (based on LDAP ACLs)
   *   <li>Password is only held in memory during the login session
   *   <li>Follows principle of least privilege
   * </ul>
   *
   * <p><b>Important:</b> The username parameter should be the login username (e.g., "jdoe"), NOT
   * the display name from the CN attribute (e.g., "Doe, John"). The CN may contain the user's full
   * name with special characters, which cannot be used for LDAP authentication.
   *
   * @param username The login username (e.g., "jdoe") - REQUIRED for LDAP authentication
   * @param userDn The Distinguished Name of the user (e.g., "CN=Doe\\,
   *     John,OU=Users,DC=example,DC=com") - used for group search
   * @param userPassword The user's password obtained during login (REQUIRED - used to authenticate
   *     to LDAP)
   * @return List of CorpGroupSnapshot objects representing the user's groups
   * @throws IllegalArgumentException if userPassword is null or empty
   */
  public List<CorpGroupSnapshot> extractGroups(
      @Nonnull String username, @Nonnull String userDn, @Nonnull String userPassword) {
    List<CorpGroupSnapshot> groups = new ArrayList<>();

    if (!configs.isExtractGroupsEnabled()) {
      log.debug("Group extraction is disabled");
      return groups;
    }

    if (configs.getGroupBaseDn() == null || configs.getGroupBaseDn().isEmpty()) {
      log.warn("Group base DN is not configured. Cannot extract groups.");
      return groups;
    }

    if (userPassword == null || userPassword.isEmpty()) {
      throw new IllegalArgumentException(
          "User password is required for LDAP operations. "
              + "Storing bindDn/bindPassword in configuration is not supported for security reasons.");
    }

    DirContext ctx = null;
    try {
      // Use the provided login username (not extracted from DN) for authentication
      String authIdentity = buildAuthIdentity(username);
      ctx = connectionFactory.createConnection(authIdentity, userPassword);
      log.debug("Using user's own credentials for group search (username: {})", username);

      return extractGroupsWithContext(ctx, userDn);

    } catch (NamingException e) {
      log.error("Failed to extract groups from LDAP: {}", e.getMessage(), e);
    } finally {
      connectionFactory.closeConnection(ctx);
    }

    return groups;
  }

  /**
   * Extracts the simple username from a Distinguished Name (DN).
   *
   * <p>This method properly handles LDAP escape sequences in DN values, which are required when the
   * CN contains special characters like commas, quotes, backslashes, etc.
   *
   * <p><b>Standard Examples:</b>
   *
   * <ul>
   *   <li>"CN=John Doe,OU=Users,DC=example,DC=com" → "John Doe"
   *   <li>"CN=jdoe,OU=Users,DC=example,DC=com" → "jdoe"
   *   <li>"CN=user123,OU=Employees,DC=corp,DC=com" → "user123"
   * </ul>
   *
   * <p><b>Escaped Character Examples:</b>
   *
   * <ul>
   *   <li>"CN=Bharti\, Aakash,OU=Regular-NonProd,..." → "Bharti, Aakash" (escaped comma)
   *   <li>"CN=Doe\, John,OU=Users,DC=example,DC=com" → "Doe, John" (escaped comma)
   *   <li>"CN=Smith\, Jane\, Jr.,OU=Users,..." → "Smith, Jane, Jr." (multiple escaped commas)
   *   <li>"CN=John\\Smith,OU=Users,..." → "John\Smith" (escaped backslash)
   *   <li>"CN=\"John Doe\",OU=Users,..." → "\"John Doe\"" (escaped quotes)
   *   <li>"CN=\<Admin\>,OU=Users,..." → "&lt;Admin&gt;" (escaped angle brackets)
   *   <li>"CN=user\+admin,OU=Users,..." → "user+admin" (escaped plus sign)
   * </ul>
   *
   * <p><b>Edge Cases Handled:</b>
   *
   * <ul>
   *   <li><b>Null or empty DN:</b> Returns empty string
   *   <li><b>DN without CN:</b> Returns the original DN unchanged
   *   <li><b>CN at end of DN (no comma after):</b> Extracts entire CN value
   *       <ul>
   *         <li>Example: "CN=John Doe" → "John Doe"
   *       </ul>
   *   <li><b>Trailing backslash:</b> Handled safely (backslash at end is ignored)
   *       <ul>
   *         <li>Example: "CN=John\\,OU=Users,..." → "John\" (backslash before comma)
   *       </ul>
   *   <li><b>Multiple consecutive escapes:</b> Each escape is processed independently
   *       <ul>
   *         <li>Example: "CN=A\\\\B,OU=..." → "A\\B" (escaped backslash twice)
   *       </ul>
   *   <li><b>Mixed escaped and unescaped characters:</b> Correctly distinguishes between them
   *       <ul>
   *         <li>Example: "CN=Doe\, John (Manager),OU=..." → "Doe, John (Manager)"
   *       </ul>
   *   <li><b>Unicode and special characters:</b> Preserved as-is
   *       <ul>
   *         <li>Example: "CN=José García,OU=..." → "José García"
   *       </ul>
   *   <li><b>Spaces in CN:</b> Preserved exactly as they appear
   *       <ul>
   *         <li>Example: "CN= John Doe ,OU=..." → " John Doe " (with spaces)
   *       </ul>
   * </ul>
   *
   * <p><b>LDAP Escaping Rules (RFC 4514):</b>
   *
   * <p>In LDAP Distinguished Names, the following characters must be escaped with a backslash when
   * they appear in attribute values:
   *
   * <ul>
   *   <li><b>Comma (,):</b> {@code \,} - Separates DN components
   *   <li><b>Plus (+):</b> {@code \+} - Separates multi-valued RDNs
   *   <li><b>Quote ("):</b> {@code \"} - String delimiter
   *   <li><b>Backslash (\):</b> {@code \\} - Escape character itself
   *   <li><b>Less than (&lt;):</b> {@code \<} - Special LDAP character
   *   <li><b>Greater than (&gt;):</b> {@code \>} - Special LDAP character
   *   <li><b>Semicolon (;):</b> {@code \;} - Alternative DN separator
   *   <li><b>Leading/trailing spaces:</b> Must be escaped if significant
   *   <li><b>Hash (#) at start:</b> {@code \#} - Indicates hex-encoded value
   * </ul>
   *
   * <p><b>Algorithm:</b>
   *
   * <ol>
   *   <li>Start parsing after "CN=" prefix
   *   <li>For each character:
   *       <ul>
   *         <li>If backslash (\): Take next character literally (unescape it)
   *         <li>If unescaped comma (,): Stop parsing (end of CN value)
   *         <li>Otherwise: Include character in result
   *       </ul>
   *   <li>Return the accumulated CN value
   * </ol>
   *
   * <p><b>Security Note:</b> This method does NOT validate the DN format or perform any
   * sanitization. It assumes the DN comes from a trusted LDAP server. For untrusted input, proper
   * validation should be performed before calling this method.
   *
   * <p><b>Performance:</b> O(n) where n is the length of the CN value. Uses StringBuilder for
   * efficient string concatenation.
   *
   * @param dn The Distinguished Name from LDAP (e.g., "CN=John Doe,OU=Users,DC=example,DC=com")
   * @return The username extracted from the CN attribute with escape sequences processed, or empty
   *     string if DN is null/empty, or the original DN if it doesn't start with "CN="
   */
  private String extractUsernameFromDn(String dn) {
    if (dn == null || dn.isEmpty()) {
      return "";
    }

    // Extract CN value from DN, handling escaped commas
    if (dn.startsWith("CN=")) {
      // Find the first unescaped comma (which marks the end of the CN value)
      int index = 3; // Start after "CN="
      StringBuilder cnValue = new StringBuilder();

      while (index < dn.length()) {
        char currentChar = dn.charAt(index);

        if (currentChar == '\\' && index + 1 < dn.length()) {
          // This is an escape character, include the next character literally
          char nextChar = dn.charAt(index + 1);
          cnValue.append(nextChar);
          index += 2; // Skip both the backslash and the escaped character
        } else if (currentChar == ',') {
          // This is an unescaped comma, marking the end of the CN value
          break;
        } else {
          // Regular character
          cnValue.append(currentChar);
          index++;
        }
      }

      return cnValue.toString();
    }

    return dn;
  }

  /**
   * Builds the authentication identity for LDAP bind.
   *
   * <p>This constructs the proper format for authentication based on the LDAP configuration. For
   * example, if the userFilter contains "@example.com", it will append that domain.
   *
   * @param username The simple username
   * @return The authentication identity (e.g., "jdoe@example.com")
   */
  private String buildAuthIdentity(String username) {
    String userFilter = configs.getUserFilter();

    // If filter contains @, extract the domain and build UPN
    if (userFilter.contains("@")) {
      int atIndex = userFilter.indexOf("@");
      int endIndex = userFilter.indexOf(")", atIndex);
      if (endIndex > atIndex) {
        String domain = userFilter.substring(atIndex + 1, endIndex);
        return username + "@" + domain;
      }
    }

    // Otherwise, just return username
    return username;
  }

  /**
   * Internal helper method that performs the actual LDAP group search using an existing directory
   * context.
   *
   * <p>This method is called by both extractGroups() methods after they've established an LDAP
   * connection (either with service account credentials or user's own credentials).
   *
   * <p><b>What it does:</b>
   *
   * <ol>
   *   <li>Configures search parameters (scope, attributes to retrieve)
   *   <li>Builds search filter from configured groupFilter (e.g., "(member={0})")
   *   <li>Replaces {0} with the user's DN (with proper escaping for LDAP filters)
   *   <li>Executes LDAP search in the configured groupBaseDn
   *   <li>Extracts group attributes (name, description, etc.)
   *   <li>Converts LDAP groups to DataHub CorpGroupSnapshot format
   * </ol>
   *
   * <p><b>DN Escaping:</b> When a DN contains special characters (like backslashes in "CN=Doe\,
   * John"), those characters must be escaped for use in LDAP search filters. This method handles
   * that escaping automatically.
   *
   * <p><b>Note:</b> This method does NOT close the context - that's the responsibility of the
   * caller.
   *
   * @param ctx The LDAP directory context (already authenticated)
   * @param userDn The user's Distinguished Name to search for in group memberships
   * @return List of CorpGroupSnapshot objects
   */
  private List<CorpGroupSnapshot> extractGroupsWithContext(DirContext ctx, String userDn) {
    List<CorpGroupSnapshot> groups = new ArrayList<>();

    try {

      // Search for groups
      SearchControls searchControls = new SearchControls();
      searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

      // Set attributes to retrieve
      String[] attributesToReturn = {
        configs.getGroupNameAttribute(), "cn", "distinguishedName", "description"
      };
      searchControls.setReturningAttributes(attributesToReturn);

      // Escape the DN for use in LDAP search filter
      // Backslashes in the DN (e.g., "CN=Doe\, John") need to be escaped as "\\" for the filter
      String escapedUserDn = escapeDnForFilter(userDn);

      // Build search filter - replace {0} with escaped user DN
      String searchFilter = configs.getGroupFilter().replace("{0}", escapedUserDn);

      log.debug(
          "Searching for groups with filter: {} in base DN: {}",
          searchFilter,
          configs.getGroupBaseDn());

      NamingEnumeration<SearchResult> results =
          ctx.search(configs.getGroupBaseDn(), searchFilter, searchControls);

      while (results.hasMore()) {
        SearchResult searchResult = results.next();
        Attributes attributes = searchResult.getAttributes();

        String groupName = getAttributeValue(attributes, configs.getGroupNameAttribute());
        if (groupName == null) {
          groupName = getAttributeValue(attributes, "cn");
        }

        if (groupName != null) {
          log.debug("Found group: {}", groupName);

          try {
            CorpGroupSnapshot groupSnapshot = buildCorpGroupSnapshot(groupName, attributes);
            groups.add(groupSnapshot);
          } catch (Exception e) {
            log.warn("Failed to build group snapshot for group {}: {}", groupName, e.getMessage());
          }
        }
      }

      log.debug("Successfully extracted {} groups", groups.size());

    } catch (NamingException e) {
      log.error("Failed to extract groups: {}", e.getMessage());
    }

    return groups;
  }

  /**
   * Builds a CorpGroupSnapshot from LDAP group attributes.
   *
   * @param groupName The name of the group
   * @param attributes The LDAP attributes
   * @return CorpGroupSnapshot
   * @throws UnsupportedEncodingException if URL encoding fails
   */
  private CorpGroupSnapshot buildCorpGroupSnapshot(String groupName, Attributes attributes)
      throws UnsupportedEncodingException {

    // URL encode the group name to handle spaces and special characters
    final String urlEncodedGroupName =
        URLEncoder.encode(groupName, StandardCharsets.UTF_8.toString());
    final CorpGroupUrn groupUrn = new CorpGroupUrn(urlEncodedGroupName);

    final CorpGroupSnapshot corpGroupSnapshot = new CorpGroupSnapshot();
    corpGroupSnapshot.setUrn(groupUrn);

    final CorpGroupAspectArray aspects = new CorpGroupAspectArray();

    // Build CorpGroupInfo aspect
    final CorpGroupInfo corpGroupInfo = new CorpGroupInfo();
    corpGroupInfo.setAdmins(new CorpuserUrnArray());
    corpGroupInfo.setGroups(new CorpGroupUrnArray());
    corpGroupInfo.setMembers(new CorpuserUrnArray());
    corpGroupInfo.setEmail("");
    corpGroupInfo.setDisplayName(groupName);

    // Set description if available
    String description = getAttributeValue(attributes, "description");
    if (description != null && !description.isEmpty()) {
      corpGroupInfo.setDescription(description);
    }

    aspects.add(CorpGroupAspect.create(corpGroupInfo));

    // Build the key
    final CorpGroupKey groupKey = new CorpGroupKey();
    groupKey.setName(urlEncodedGroupName);
    aspects.add(CorpGroupAspect.create(groupKey));

    corpGroupSnapshot.setAspects(aspects);

    return corpGroupSnapshot;
  }

  /**
   * Escapes a Distinguished Name for use in an LDAP search filter.
   *
   * <p>When a DN is used in an LDAP search filter, certain characters need to be escaped. Most
   * importantly, backslashes in the DN (which are used to escape special characters in the DN
   * itself) must be escaped again for the filter.
   *
   * <p><b>Example:</b>
   *
   * <ul>
   *   <li>DN from LDAP: {@code CN=Doe\, John,OU=Users,DC=example,DC=com}
   *   <li>Escaped for filter: {@code CN=Doe\\, John,OU=Users,DC=example,DC=com}
   * </ul>
   *
   * <p><b>Why this is needed:</b> In LDAP filters, backslashes have special meaning and must be
   * escaped. When a DN contains {@code \,} (escaped comma), the backslash itself needs to be
   * escaped as {@code \\} in the filter, resulting in {@code \\,}.
   *
   * <p><b>Characters escaped:</b>
   *
   * <ul>
   *   <li>{@code \} → {@code \\} (backslash)
   *   <li>{@code *} → {@code \*} (asterisk - wildcard in filters)
   *   <li>{@code (} → {@code \(} (left parenthesis)
   *   <li>{@code )} → {@code \)} (right parenthesis)
   *   <li>{@code \0} → {@code \00} (null character)
   * </ul>
   *
   * @param dn The Distinguished Name to escape
   * @return The escaped DN safe for use in LDAP search filters
   */
  private String escapeDnForFilter(String dn) {
    if (dn == null || dn.isEmpty()) {
      return dn;
    }

    // Escape special characters for LDAP filter
    // Most importantly: backslash must be escaped as \\
    StringBuilder escaped = new StringBuilder();

    for (int i = 0; i < dn.length(); i++) {
      char c = dn.charAt(i);
      switch (c) {
        case '\\':
          escaped.append("\\\\"); // Backslash → \\
          break;
        case '*':
          escaped.append("\\*"); // Asterisk → \*
          break;
        case '(':
          escaped.append("\\("); // Left paren → \(
          break;
        case ')':
          escaped.append("\\)"); // Right paren → \)
          break;
        case '\0':
          escaped.append("\\00"); // Null → \00
          break;
        default:
          escaped.append(c);
      }
    }

    return escaped.toString();
  }

  /**
   * Gets the value of an LDAP attribute.
   *
   * @param attributes The attributes collection
   * @param attributeName The name of the attribute to retrieve
   * @return The attribute value as a string, or null if not found
   */
  private String getAttributeValue(Attributes attributes, String attributeName) {
    try {
      Attribute attribute = attributes.get(attributeName);
      if (attribute != null && attribute.get() != null) {
        return attribute.get().toString();
      }
    } catch (NamingException e) {
      log.debug("Could not retrieve attribute {}: {}", attributeName, e.getMessage());
    }
    return null;
  }
}
