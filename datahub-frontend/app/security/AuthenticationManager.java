package security;

import auth.ldap.LdapConnectionUtil;
import auth.ldap.LdapUserAttributeExtractor;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthenticationManager {
  private static final Logger log = LoggerFactory.getLogger(AuthenticationManager.class);

  private AuthenticationManager() {} // Prevent instantiation

  /**
   * Performs a direct LDAP query to find groups for a user.
   *
   * @param userDN The user's distinguished name
   * @return A set of group names the user belongs to
   */
  public static Set<String> queryUserGroups(
      @Nonnull String userDN,
      @Nonnull java.util.Map<String, String> options,
      @Nonnull String userName,
      @Nonnull String password) {

    // Validate required parameters
    if (userDN == null) {
      throw new NullPointerException("userDN cannot be null");
    }
    if (options == null) {
      throw new NullPointerException("options cannot be null");
    }
    if (userName == null) {
      throw new NullPointerException("userName cannot be null");
    }
    if (password == null) {
      throw new NullPointerException("password cannot be null");
    }

    Set<String> groups = new HashSet<>();
    log.info("Querying groups for user DN: {}", userDN);

    try {
      // Use centralized LDAP environment creation
      Hashtable<String, Object> env =
          LdapConnectionUtil.createLdapEnvironment(options, userName, password);
      String baseDN = extractBaseDN(options.getOrDefault("userProvider", ""));

      // Log LDAP configuration for debugging
      LdapConnectionUtil.logLdapEnvironment(env);
      log.info("  Base DN: {}", baseDN);

      log.info(
          "Attempting to create LDAP context with principal: {}",
          env.get(Context.SECURITY_PRINCIPAL));

      try {
        // Create the initial context using utility method
        DirContext ctx = LdapConnectionUtil.createLdapContext(env);
        log.info("Successfully created LDAP context");

        // Create the search controls
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        String escapedUserDN = userDN.replace("\\,", "\\\\,");
        String[] groupFilters =
            new String[] {"(&(objectClass=group)(member=" + escapedUserDN + "))"};

        for (String filter : groupFilters) {
          try {
            log.info("Trying group filter: {}", filter);
            NamingEnumeration<SearchResult> results = ctx.search(baseDN, filter, searchControls);

            int resultCount = 0;
            while (results.hasMore()) {
              SearchResult result = results.next();
              resultCount++;
              Attributes attrs = result.getAttributes();
              Attribute groupAttr = attrs.get(options.getOrDefault("groupNameAttribute", "cn"));

              if (groupAttr != null) {
                String groupName = groupAttr.get().toString();
                groups.add(groupName);
                log.info("Found group: {}", groupName);
              }
            }

            log.info("LDAP search returned {} results for filter: {}", resultCount, filter);

            // If we found groups with this filter, no need to try others
            if (!groups.isEmpty()) {
              log.info("Successfully found {} groups with filter: {}", groups.size(), filter);
              break;
            } else {
              log.info("No groups found with filter: {}", filter);
            }
          } catch (NamingException e) {
            log.warn("Filter {} failed: {}", filter, e.getMessage());
          }
        }

        ctx.close();
      } catch (javax.naming.AuthenticationException ae) {
        log.error("LDAP Authentication failed: {}", ae.getMessage(), ae);
        log.error("Authentication Error Explanation: {}", ae.getExplanation());
        // Try with a different format for the principal
        try {
          log.info("Retrying with DN format for principal");
          env.put(Context.SECURITY_PRINCIPAL, options.getOrDefault("fallbackPrincipal", ""));
          DirContext ctx2 = new InitialDirContext(env);
          log.info("Successfully created LDAP context with DN format");

          // Create the search controls
          SearchControls searchControls = new SearchControls();
          searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

          // Use groupFilter from options if present, otherwise default filters
          String[] groupFilters;
          if (options.containsKey("groupFilter")) {
            groupFilters = new String[] {options.get("groupFilter").replace("{USER_DN}", userDN)};
          } else {
            groupFilters =
                new String[] {
                  "(&(objectClass=group)(member=" + userDN + "))",
                  "(member=" + userDN + ")",
                  "(&(objectClass=groupOfNames)(member=" + userDN + "))",
                  "(&(objectClass=group)(uniqueMember=" + userDN + "))"
                };
          }

          for (String filter : groupFilters) {
            try {
              log.info("Trying group filter with DN format: {}", filter);
              NamingEnumeration<SearchResult> results = ctx2.search(baseDN, filter, searchControls);

              while (results.hasMore()) {
                SearchResult result = results.next();
                Attributes attrs = result.getAttributes();
                Attribute groupAttr = attrs.get(options.getOrDefault("groupNameAttribute", "cn"));

                if (groupAttr != null) {
                  String groupName = groupAttr.get().toString();
                  groups.add(groupName);
                  log.info("Found group: {}", groupName);
                }
              }

              // If we found groups with this filter, no need to try others
              if (!groups.isEmpty()) {
                log.info("Successfully found {} groups with filter: {}", groups.size(), filter);
                break;
              }
            } catch (NamingException e) {
              log.warn("Filter {} failed with DN format: {}", filter, e.getMessage());
            }
          }

          ctx2.close();
        } catch (NamingException ne) {
          log.error("LDAP Authentication failed with DN format: {}", ne.getMessage(), ne);
        }
      }
    } catch (NamingException e) {
      log.error("Error querying LDAP for groups: {}", e.getMessage(), e);
      if (e instanceof javax.naming.CommunicationException) {
        log.error("LDAP Communication error - check server connectivity and SSL settings");
      } else if (e instanceof javax.naming.AuthenticationException) {
        log.error("LDAP Authentication error - check credentials");
      } else if (e instanceof javax.naming.NoPermissionException) {
        log.error("LDAP Permission error - check account permissions");
      }
    }

    return groups;
  }

  // Helper to extract baseDN from LDAP URL
  public static String extractBaseDN(String ldapUrl) {
    if (ldapUrl == null) return "";

    // Handle LDAP URLs like "ldaps://server.com:636/DC=DOMAIN,DC=COM"
    // We need to find the first "/" after the protocol "://"
    int protocolEnd = ldapUrl.indexOf("://");
    if (protocolEnd == -1) {
      log.warn("Could not extract baseDN from URL: {}", ldapUrl);
      return "";
    }

    // Look for "/" after the protocol part
    int idx = ldapUrl.indexOf("/", protocolEnd + 3);
    if (idx >= 0 && idx + 1 < ldapUrl.length()) {
      String baseDN = ldapUrl.substring(idx + 1);
      log.info("Extracted baseDN: {} from URL: {}", baseDN, ldapUrl);
      return baseDN;
    }

    log.warn("Could not extract baseDN from URL: {}", ldapUrl);
    return "";
  }

  /**
   * Authenticates a user and retrieves their group memberships.
   *
   * @param userName The username
   * @param password The password
   * @return A set of group names the user belongs to
   * @throws Exception If authentication fails
   */
  public static AuthResult authenticateAndGetGroupsAndSubject(
      @Nonnull String userName, @Nonnull String password) throws Exception {

    // Validate input parameters
    if (userName == null || userName.trim().isEmpty()) {
      throw new IllegalArgumentException("Username cannot be empty");
    }

    if (password == null || password.trim().isEmpty()) {
      throw new IllegalArgumentException("Password cannot be empty");
    }

    // Create a login context with our custom callback handler
    LoginContext loginContext =
        new LoginContext("WHZ-Authentication", new WHZCallbackHandler(userName, password));

    try {
      // Attempt login
      loginContext.login();
      log.debug("Authentication succeeded for user: {}", userName);

      // Extract LDAP options from the JAAS configuration file
      Map<String, String> ldapOptions = LdapConnectionUtil.extractLdapOptionsFromJaasConfig();

      // Get the authenticated subject
      Subject subject = loginContext.getSubject();

      // First check if groups are already in the Subject (since createGroupPrincipals="true")
      Set<String> groupsFromSubject = extractGroupsFromSubject(subject);
      String userDN = extractUserDN(subject);

      Set<String> groups = new HashSet<>();
      if (!groupsFromSubject.isEmpty()) {
        log.info(
            "Successfully found {} groups in Subject, skipping LDAP query",
            groupsFromSubject.size());
        groups = groupsFromSubject;
      } else if (userDN != null) {
        log.info("No groups found in Subject, trying LDAP query with extracted options");
        groups = queryUserGroups(userDN, ldapOptions, userName, password);
      }

      return new AuthResult(groups, userDN, ldapOptions, subject);

    } catch (LoginException le) {
      log.info("Authentication failed for user {}: {}", userName, le.getMessage());
      AuthenticationException authenticationException =
          new AuthenticationException(le.getMessage());
      authenticationException.setRootCause(le);
      throw authenticationException;
    }
  }

  public static class AuthResult {
    public final Set<String> groups;
    public final String userDN;
    public final Map<String, String> ldapOptions;
    public final Subject subject;

    public AuthResult(
        Set<String> groups, String userDN, Map<String, String> ldapOptions, Subject subject) {
      this.groups = groups;
      this.userDN = userDN;
      this.ldapOptions = ldapOptions;
      this.subject = subject;
    }
  }

  /**
   * Extracts group names from the Subject. The JAAS LdapLoginModule should have populated the
   * Subject with group principals if createGroupPrincipals="true" was set in the JAAS config.
   *
   * @param subject The authenticated Subject
   * @return A set of group names
   */
  private static Set<String> extractGroupsFromSubject(Subject subject) {
    Set<String> groups = new HashSet<>();
    if (subject == null) {
      return groups;
    }

    log.debug("Extracting groups from Subject with {} principals", subject.getPrincipals().size());

    // Log all principal types and names for debugging
    for (java.security.Principal principal : subject.getPrincipals()) {
      String principalName = principal.getName();
      String principalClass = principal.getClass().getName();
      log.debug("Found principal: {} of type {}", principalName, principalClass);

      // Check for common group principal classes
      if (principalClass.contains("Group") || principalClass.contains("Role")) {
        log.debug("Adding group principal: {}", principalName);
        groups.add(principalName);
      }
    }

    log.info("Extracted {} groups from Subject", groups.size());
    return groups;
  }

  /**
   * Extracts the user's Distinguished Name (DN) from the Subject.
   *
   * @param subject The authenticated Subject
   * @return The user's DN, or null if not found
   */
  public static String extractUserDN(Subject subject) {
    if (subject == null) {
      return null;
    }

    log.debug("Extracting user DN from Subject with {} principals", subject.getPrincipals().size());

    // Log all principals for debugging
    for (java.security.Principal principal : subject.getPrincipals()) {
      String name = principal.getName();
      String className = principal.getClass().getName();
      log.debug("Principal: {} (type: {})", name, className);
    }

    // Look for a principal that looks like a DN (contains CN= and DC=)
    for (java.security.Principal principal : subject.getPrincipals()) {
      String name = principal.getName();
      if (name != null && name.contains("CN=") && name.contains("DC=")) {
        log.info("Found user DN: {}", name);
        return name;
      }
    }

    log.warn("No DN-like principal found in Subject");
    return null;
  }

  // END GENAI

  public static Map<String, String> getUserAttributesFromLdap(
      String userDN,
      Map<String, String> ldapOptions,
      @Nonnull String username,
      @Nonnull String password) {
    return LdapUserAttributeExtractor.fetchLdapUserAttributes(
        userDN, ldapOptions, username, password);
  }

  private static class WHZCallbackHandler implements CallbackHandler {
    private final String password;
    private final String username;

    private WHZCallbackHandler(@Nonnull String username, @Nonnull String password) {
      this.username = username;
      this.password = password;
    }

    @Override
    public void handle(@Nonnull Callback[] callbacks) {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nc = (NameCallback) callback;
          nc.setName(username);
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback pc = (PasswordCallback) callback;
          pc.setPassword(password.toCharArray());
        }
      }
    }
  }
}
