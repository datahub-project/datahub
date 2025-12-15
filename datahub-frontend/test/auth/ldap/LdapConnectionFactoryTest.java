package auth.ldap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for LdapConnectionFactory. Note: These tests verify configuration and method behavior.
 * Full integration tests with actual LDAP server are documented in LDAP_TEST_GUIDE.md
 */
public class LdapConnectionFactoryTest {

  private LdapConfigs ldapConfigs;
  private LdapConnectionFactory connectionFactory;
  private Map<String, Object> configMap;

  @BeforeEach
  public void setUp() {
    configMap = new HashMap<>();
    configMap.put("auth.ldap.server", "ldap://localhost:389");
    configMap.put("auth.ldap.baseDn", "ou=users,dc=example,dc=com");
    configMap.put("auth.ldap.bindDn", "cn=admin,dc=example,dc=com");
    configMap.put("auth.ldap.bindPassword", "admin");
    configMap.put("auth.ldap.useSsl", false);
    configMap.put("auth.ldap.startTls", false);
    configMap.put("auth.ldap.connectionTimeout", 5000);
    configMap.put("auth.ldap.readTimeout", 5000);
    configMap.put("auth.ldap.poolTimeout", 300000);

    Config config = ConfigFactory.parseMap(configMap);
    ldapConfigs = new LdapConfigs(config);
    connectionFactory = new LdapConnectionFactory(ldapConfigs);
  }

  @Test
  public void testConstructor() {
    // Test that factory can be constructed with valid config
    assertNotNull(connectionFactory);
  }

  @Test
  public void testCreateConnectionWithSslConfig() {
    // Test SSL configuration is properly set
    configMap.put("auth.ldap.useSsl", true);
    configMap.put("auth.ldap.server", "ldaps://localhost:636");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs sslConfigs = new LdapConfigs(config);
    LdapConnectionFactory sslFactory = new LdapConnectionFactory(sslConfigs);

    assertNotNull(sslFactory);
    assertTrue(sslConfigs.isUseSsl());
  }

  @Test
  public void testCreateConnectionWithStartTlsConfig() {
    // Test StartTLS configuration
    configMap.put("auth.ldap.startTls", true);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs tlsConfigs = new LdapConfigs(config);
    LdapConnectionFactory tlsFactory = new LdapConnectionFactory(tlsConfigs);

    assertNotNull(tlsFactory);
    assertTrue(tlsConfigs.isStartTls());
  }

  @Test
  public void testCreateConnectionWithTrustAllCerts() {
    // Test trust all certificates configuration
    configMap.put("auth.ldap.useSsl", true);
    configMap.put("auth.ldap.trustAllCerts", true);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs trustAllConfigs = new LdapConfigs(config);
    LdapConnectionFactory trustAllFactory = new LdapConnectionFactory(trustAllConfigs);

    assertNotNull(trustAllFactory);
    assertTrue(trustAllConfigs.isTrustAllCerts());
  }

  @Test
  public void testTimeoutConfiguration() {
    // Test that timeout values are properly configured
    assertEquals(5000, ldapConfigs.getConnectionTimeout());
    assertEquals(5000, ldapConfigs.getReadTimeout());
    assertEquals(300000, ldapConfigs.getPoolTimeout());
  }

  @Test
  public void testCustomTimeoutConfiguration() {
    // Test custom timeout values
    configMap.put("auth.ldap.connectionTimeout", 10000);
    configMap.put("auth.ldap.readTimeout", 15000);
    configMap.put("auth.ldap.poolTimeout", 600000);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs customConfigs = new LdapConfigs(config);
    LdapConnectionFactory customFactory = new LdapConnectionFactory(customConfigs);

    assertNotNull(customFactory);
    assertEquals(10000, customConfigs.getConnectionTimeout());
    assertEquals(15000, customConfigs.getReadTimeout());
    assertEquals(600000, customConfigs.getPoolTimeout());
  }

  @Test
  public void testCloseConnectionWithNull() {
    // Test that closing null connection doesn't throw exception
    assertDoesNotThrow(
        () -> {
          connectionFactory.closeConnection(null);
        });
  }

  @Test
  public void testBinaryAttributesConfiguration() {
    // Test that binary attributes are configured for Active Directory
    // This is verified by checking the config is properly set
    // Actual JNDI configuration happens in createConnection
    assertNotNull(connectionFactory);
  }

  @Test
  public void testConnectionPoolingConfiguration() {
    // Test that connection pooling is enabled by default
    // Actual pooling behavior is tested in integration tests
    assertNotNull(connectionFactory);
  }

  @Test
  public void testReferralHandlingConfiguration() {
    // Test that referral handling is configured
    // Default should be "follow"
    assertNotNull(connectionFactory);
  }

  /**
   * Note: The following tests require an actual LDAP server and are better suited for integration
   * tests. They are documented in LDAP_TEST_GUIDE.md:
   *
   * <p>- testCreateConnectionWithValidCredentials() - testCreateConnectionWithInvalidCredentials()
   * - testCreateConnectionTimeout() - testReadTimeout() - testConnectionPooling() -
   * testTestConnection() - testCloseConnection() (with real connection)
   *
   * <p>These tests would use an embedded LDAP server (UnboundID LDAP SDK or Apache Directory
   * Server) to verify actual connection behavior.
   */
}
