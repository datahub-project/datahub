package auth.ldap;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import security.AuthenticationManager;

/**
 * Utility for extracting LDAP user attributes (mail, displayName, givenName, sn, etc.) given a user
 * DN. Does not modify AuthenticationManager.
 */
public class LdapUserAttributeExtractor {
  private static final Logger log = LoggerFactory.getLogger(LdapUserAttributeExtractor.class);

  /**
   * Fetches LDAP user attributes for a given user DN.
   *
   * @param userDN The user's distinguished name
   * @param options LDAP connection options
   * @return Map of attribute name to value
   */
  public static Map<String, String> fetchLdapUserAttributes(
      String userDN,
      Map<String, String> options,
      @Nonnull String username,
      @Nonnull String password) {
    Map<String, String> attributesMap = new HashMap<>();
    log.info("Fetching LDAP attributes for user DN: {}", userDN);

    // Handle null options gracefully
    if (options == null) {
      log.warn("LDAP options are null, returning empty attributes map");
      return attributesMap;
    }

    DirContext ctx = null;
    try {
      // Use centralized LDAP environment creation for service account access
      Hashtable<String, Object> env =
          LdapConnectionUtil.createLdapEnvironment(options, username, password);
      ctx = LdapConnectionUtil.createLdapContext(env);

      // Search for the user by DN
      String baseDN = AuthenticationManager.extractBaseDN(options.getOrDefault("userProvider", ""));
      SearchControls searchControls = new SearchControls();
      searchControls.setSearchScope(SearchControls.OBJECT_SCOPE); // Only the user object

      NamingEnumeration<SearchResult> results =
          ctx.search(baseDN, "(objectClass=person)", searchControls);

      if (results.hasMore()) {
        SearchResult result = results.next();
        Attributes attrs = result.getAttributes();

        // Common LDAP attributes
        String[] wantedAttrs =
            new String[] {"mail", "displayName", "givenName", "sn", "cn", "userPrincipalName"};
        for (String attrName : wantedAttrs) {
          Attribute attr = attrs.get(attrName);
          if (attr != null) {
            attributesMap.put(attrName, attr.get().toString());
          }
        }
      } else {
        log.warn("No LDAP user found for DN: {}", userDN);
      }
    } catch (NamingException e) {
      log.error("Error fetching LDAP user attributes for DN {}: {}", userDN, e.getMessage(), e);
    } finally {
      if (ctx != null) {
        try {
          ctx.close();
        } catch (NamingException e) {
          log.warn("Failed to close LDAP context: {}", e.getMessage());
        }
      }
    }

    return attributesMap;
  }
}
