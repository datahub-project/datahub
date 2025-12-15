package auth.ldap;

import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for LdapConfigs class. Tests configuration loading from both application.conf and
 * environment variables.
 */
public class LdapConfigsTest {

  private Map<String, Object> configMap;

  @BeforeEach
  public void setUp() {
    configMap = new HashMap<>();
  }

  @Test
  public void testDefaultConfiguration() {
    // Test with minimal configuration
    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    assertFalse(ldapConfigs.isEnabled());
    assertEquals("", ldapConfigs.getServer());
    assertEquals("", ldapConfigs.getBaseDn());
    assertEquals("(uid={0})", ldapConfigs.getUserFilter());
    assertEquals("uid", ldapConfigs.getUserIdAttribute());
    assertFalse(ldapConfigs.isUseSsl());
    assertFalse(ldapConfigs.isStartTls());
    assertTrue(ldapConfigs.isJitProvisioningEnabled());
    assertFalse(ldapConfigs.isPreProvisioningRequired());
    assertFalse(ldapConfigs.isExtractGroupsEnabled());
  }

  @Test
  public void testBasicLdapConfiguration() {
    // Test basic LDAP configuration
    configMap.put("auth.ldap.enabled", true);
    configMap.put("auth.ldap.server", "ldap://ldap.example.com:389");
    configMap.put("auth.ldap.baseDn", "ou=users,dc=example,dc=com");
    configMap.put("auth.ldap.bindDn", "cn=admin,dc=example,dc=com");
    configMap.put("auth.ldap.bindPassword", "admin-password");
    configMap.put("auth.ldap.userFilter", "(uid={0})");
    configMap.put("auth.ldap.userIdAttribute", "uid");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    assertTrue(ldapConfigs.isEnabled());
    assertEquals("ldap://ldap.example.com:389", ldapConfigs.getServer());
    assertEquals("ou=users,dc=example,dc=com", ldapConfigs.getBaseDn());
    assertEquals("cn=admin,dc=example,dc=com", ldapConfigs.getBindDn());
    assertEquals("admin-password", ldapConfigs.getBindPassword());
    assertEquals("(uid={0})", ldapConfigs.getUserFilter());
    assertEquals("uid", ldapConfigs.getUserIdAttribute());
  }

  @Test
  public void testActiveDirectoryConfiguration() {
    // Test Active Directory specific configuration
    configMap.put("auth.ldap.enabled", true);
    configMap.put("auth.ldap.server", "ldaps://ad.example.com:636");
    configMap.put("auth.ldap.baseDn", "CN=Users,DC=example,DC=com");
    configMap.put("auth.ldap.bindDn", "CN=Service Account,CN=Users,DC=example,DC=com");
    configMap.put("auth.ldap.bindPassword", "service-password");
    configMap.put("auth.ldap.userFilter", "(sAMAccountName={0})");
    configMap.put("auth.ldap.userIdAttribute", "sAMAccountName");
    configMap.put("auth.ldap.useSsl", true);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    assertTrue(ldapConfigs.isEnabled());
    assertEquals("ldaps://ad.example.com:636", ldapConfigs.getServer());
    assertEquals("CN=Users,DC=example,DC=com", ldapConfigs.getBaseDn());
    assertEquals("(sAMAccountName={0})", ldapConfigs.getUserFilter());
    assertEquals("sAMAccountName", ldapConfigs.getUserIdAttribute());
    assertTrue(ldapConfigs.isUseSsl());
  }

  @Test
  public void testSslTlsConfiguration() {
    // Test SSL/TLS configuration options
    configMap.put("auth.ldap.useSsl", true);
    configMap.put("auth.ldap.startTls", false);
    configMap.put("auth.ldap.trustAllCerts", true);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    assertTrue(ldapConfigs.isUseSsl());
    assertFalse(ldapConfigs.isStartTls());
    assertTrue(ldapConfigs.isTrustAllCerts());
  }

  @Test
  public void testTimeoutConfiguration() {
    // Test timeout configuration
    configMap.put("auth.ldap.connectionTimeout", 10000);
    configMap.put("auth.ldap.readTimeout", 15000);
    configMap.put("auth.ldap.poolTimeout", 600000);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    assertEquals(10000, ldapConfigs.getConnectionTimeout());
    assertEquals(15000, ldapConfigs.getReadTimeout());
    assertEquals(600000, ldapConfigs.getPoolTimeout());
  }

  @Test
  public void testDefaultTimeoutValues() {
    // Test default timeout values
    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    assertEquals(5000, ldapConfigs.getConnectionTimeout());
    assertEquals(5000, ldapConfigs.getReadTimeout());
    assertEquals(300000, ldapConfigs.getPoolTimeout());
  }

  @Test
  public void testUserAttributeMapping() {
    // Test user attribute mapping configuration
    configMap.put("auth.ldap.userFirstNameAttribute", "givenName");
    configMap.put("auth.ldap.userLastNameAttribute", "sn");
    configMap.put("auth.ldap.userEmailAttribute", "mail");
    configMap.put("auth.ldap.userDisplayNameAttribute", "displayName");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    assertEquals("givenName", ldapConfigs.getUserFirstNameAttribute());
    assertEquals("sn", ldapConfigs.getUserLastNameAttribute());
    assertEquals("mail", ldapConfigs.getUserEmailAttribute());
    assertEquals("displayName", ldapConfigs.getUserDisplayNameAttribute());
  }

  @Test
  public void testProvisioningConfiguration() {
    // Test provisioning configuration
    configMap.put("auth.ldap.jitProvisioningEnabled", false);
    configMap.put("auth.ldap.preProvisioningRequired", true);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    assertFalse(ldapConfigs.isJitProvisioningEnabled());
    assertTrue(ldapConfigs.isPreProvisioningRequired());
  }

  @Test
  public void testGroupExtractionConfiguration() {
    // Test group extraction configuration
    configMap.put("auth.ldap.extractGroupsEnabled", true);
    configMap.put("auth.ldap.groupBaseDn", "ou=groups,dc=example,dc=com");
    configMap.put("auth.ldap.groupFilter", "(member={0})");
    configMap.put("auth.ldap.groupNameAttribute", "cn");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    assertTrue(ldapConfigs.isExtractGroupsEnabled());
    assertEquals("ou=groups,dc=example,dc=com", ldapConfigs.getGroupBaseDn());
    assertEquals("(member={0})", ldapConfigs.getGroupFilter());
    assertEquals("cn", ldapConfigs.getGroupNameAttribute());
  }

  @Test
  public void testCompleteConfiguration() {
    // Test complete configuration with all options
    configMap.put("auth.ldap.enabled", true);
    configMap.put("auth.ldap.server", "ldaps://ldap.example.com:636");
    configMap.put("auth.ldap.baseDn", "ou=users,dc=example,dc=com");
    configMap.put("auth.ldap.bindDn", "cn=admin,dc=example,dc=com");
    configMap.put("auth.ldap.bindPassword", "admin-password");
    configMap.put("auth.ldap.userFilter", "(uid={0})");
    configMap.put("auth.ldap.userIdAttribute", "uid");
    configMap.put("auth.ldap.useSsl", true);
    configMap.put("auth.ldap.startTls", false);
    configMap.put("auth.ldap.trustAllCerts", false);
    configMap.put("auth.ldap.connectionTimeout", 10000);
    configMap.put("auth.ldap.readTimeout", 15000);
    configMap.put("auth.ldap.poolTimeout", 600000);
    configMap.put("auth.ldap.userFirstNameAttribute", "givenName");
    configMap.put("auth.ldap.userLastNameAttribute", "sn");
    configMap.put("auth.ldap.userEmailAttribute", "mail");
    configMap.put("auth.ldap.userDisplayNameAttribute", "displayName");
    configMap.put("auth.ldap.jitProvisioningEnabled", true);
    configMap.put("auth.ldap.preProvisioningRequired", false);
    configMap.put("auth.ldap.extractGroupsEnabled", true);
    configMap.put("auth.ldap.groupBaseDn", "ou=groups,dc=example,dc=com");
    configMap.put("auth.ldap.groupFilter", "(member={0})");
    configMap.put("auth.ldap.groupNameAttribute", "cn");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs ldapConfigs = new LdapConfigs(config);

    // Verify all configurations
    assertTrue(ldapConfigs.isEnabled());
    assertEquals("ldaps://ldap.example.com:636", ldapConfigs.getServer());
    assertEquals("ou=users,dc=example,dc=com", ldapConfigs.getBaseDn());
    assertEquals("cn=admin,dc=example,dc=com", ldapConfigs.getBindDn());
    assertEquals("admin-password", ldapConfigs.getBindPassword());
    assertEquals("(uid={0})", ldapConfigs.getUserFilter());
    assertEquals("uid", ldapConfigs.getUserIdAttribute());
    assertTrue(ldapConfigs.isUseSsl());
    assertFalse(ldapConfigs.isStartTls());
    assertFalse(ldapConfigs.isTrustAllCerts());
    assertEquals(10000, ldapConfigs.getConnectionTimeout());
    assertEquals(15000, ldapConfigs.getReadTimeout());
    assertEquals(600000, ldapConfigs.getPoolTimeout());
    assertEquals("givenName", ldapConfigs.getUserFirstNameAttribute());
    assertEquals("sn", ldapConfigs.getUserLastNameAttribute());
    assertEquals("mail", ldapConfigs.getUserEmailAttribute());
    assertEquals("displayName", ldapConfigs.getUserDisplayNameAttribute());
    assertTrue(ldapConfigs.isJitProvisioningEnabled());
    assertFalse(ldapConfigs.isPreProvisioningRequired());
    assertTrue(ldapConfigs.isExtractGroupsEnabled());
    assertEquals("ou=groups,dc=example,dc=com", ldapConfigs.getGroupBaseDn());
    assertEquals("(member={0})", ldapConfigs.getGroupFilter());
    assertEquals("cn", ldapConfigs.getGroupNameAttribute());
  }

  @Test
  public void testInvalidTimeoutValues() {
    // Test that invalid timeout values (strings instead of numbers) throw ConfigException
    configMap.put("auth.ldap.connectionTimeout", "invalid");
    configMap.put("auth.ldap.readTimeout", "not-a-number");

    Config config = ConfigFactory.parseMap(configMap);

    // TypeSafe Config will throw ConfigException$WrongType for type mismatches
    assertThrows(
        com.typesafe.config.ConfigException.WrongType.class,
        () -> new LdapConfigs(config),
        "Should throw ConfigException when timeout values are strings instead of numbers");
  }

  @Test
  public void testEmptyConfiguration() {
    // Test with completely empty configuration
    Config config = ConfigFactory.parseMap(configMap);

    assertDoesNotThrow(
        () -> {
          LdapConfigs ldapConfigs = new LdapConfigs(config);
          assertNotNull(ldapConfigs);
        });
  }
}
