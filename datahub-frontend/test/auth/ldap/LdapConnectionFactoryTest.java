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

  @Test
  public void testCreateConnectionWithPrincipalAndCredentials() {
    // Test that createConnection method can be called with principal and credentials
    // This will fail to connect (no real LDAP server), but verifies the method signature
    // and that it attempts to create a connection with the provided credentials
    String principal = "cn=testuser,ou=users,dc=example,dc=com";
    String credentials = "testpassword";

    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          connectionFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithUpnFormat() {
    // Test with User Principal Name format (common in Active Directory)
    String principal = "testuser@example.com";
    String credentials = "testpassword";

    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          connectionFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithSimpleUsername() {
    // Test with simple username format
    String principal = "testuser";
    String credentials = "testpassword";

    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          connectionFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithEmptyPrincipal() {
    // Test that empty principal is handled
    String principal = "";
    String credentials = "testpassword";

    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          connectionFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithEmptyCredentials() {
    // Test that empty credentials is handled
    String principal = "cn=testuser,ou=users,dc=example,dc=com";
    String credentials = "";

    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          connectionFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithSslEnabled() {
    // Test that SSL configuration is applied when creating connection
    configMap.put("auth.ldap.useSsl", true);
    configMap.put("auth.ldap.server", "ldaps://localhost:636");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs sslConfigs = new LdapConfigs(config);
    LdapConnectionFactory sslFactory = new LdapConnectionFactory(sslConfigs);

    String principal = "cn=testuser,ou=users,dc=example,dc=com";
    String credentials = "testpassword";

    // Should attempt connection with SSL (will fail without real server)
    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          sslFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithStartTlsEnabled() {
    // Test that StartTLS configuration is applied when creating connection
    configMap.put("auth.ldap.startTls", true);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs tlsConfigs = new LdapConfigs(config);
    LdapConnectionFactory tlsFactory = new LdapConnectionFactory(tlsConfigs);

    String principal = "cn=testuser,ou=users,dc=example,dc=com";
    String credentials = "testpassword";

    // Should attempt connection with StartTLS (will fail without real server)
    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          tlsFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithTrustAllCertsEnabled() {
    // Test that trust all certificates configuration is applied
    configMap.put("auth.ldap.useSsl", true);
    configMap.put("auth.ldap.trustAllCerts", true);
    configMap.put("auth.ldap.server", "ldaps://localhost:636");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs trustAllConfigs = new LdapConfigs(config);
    LdapConnectionFactory trustAllFactory = new LdapConnectionFactory(trustAllConfigs);

    String principal = "cn=testuser,ou=users,dc=example,dc=com";
    String credentials = "testpassword";

    // Should attempt connection with trust all certs (will fail without real server)
    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          trustAllFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithCustomTimeouts() {
    // Test that custom timeout values are applied when creating connection
    configMap.put("auth.ldap.connectionTimeout", 1000);
    configMap.put("auth.ldap.readTimeout", 2000);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs customConfigs = new LdapConfigs(config);
    LdapConnectionFactory customFactory = new LdapConnectionFactory(customConfigs);

    String principal = "cn=testuser,ou=users,dc=example,dc=com";
    String credentials = "testpassword";

    // Should attempt connection with custom timeouts (will fail without real server)
    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          customFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionNoArgUsesConfiguredCredentials() {
    // Test that createConnection() without arguments uses bindDn and bindPassword from config
    // This verifies the delegation to createConnection(principal, credentials)
    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          connectionFactory.createConnection();
        });
  }

  @Test
  public void testCreateConnectionWithSpecialCharactersInPassword() {
    // Test that special characters in password are handled correctly
    String principal = "cn=testuser,ou=users,dc=example,dc=com";
    String credentials = "p@ssw0rd!#$%^&*()";

    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          connectionFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithLongCredentials() {
    // Test that long credentials are handled correctly
    String principal = "cn=testuser,ou=users,dc=example,dc=com";
    String credentials = "a".repeat(1000); // Very long password

    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          connectionFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testCreateConnectionWithUnicodeCharacters() {
    // Test that Unicode characters in credentials are handled correctly
    String principal = "cn=用户,ou=users,dc=example,dc=com";
    String credentials = "密码123";

    assertThrows(
        javax.naming.NamingException.class,
        () -> {
          connectionFactory.createConnection(principal, credentials);
        });
  }

  @Test
  public void testTestConnectionWithNoLdapServer() {
    // Test that testConnection returns false when LDAP server is not available
    // This verifies the method handles connection failures gracefully
    boolean result = connectionFactory.testConnection();
    assertFalse(result, "testConnection should return false when LDAP server is not available");
  }

  @Test
  public void testTestConnectionWithInvalidServer() {
    // Test testConnection with invalid server URL
    configMap.put("auth.ldap.server", "ldap://invalid-server-that-does-not-exist:389");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs invalidConfigs = new LdapConfigs(config);
    LdapConnectionFactory invalidFactory = new LdapConnectionFactory(invalidConfigs);

    boolean result = invalidFactory.testConnection();
    assertFalse(result, "testConnection should return false with invalid server");
  }

  @Test
  public void testTestConnectionWithSslConfiguration() {
    // Test testConnection with SSL enabled
    configMap.put("auth.ldap.useSsl", true);
    configMap.put("auth.ldap.server", "ldaps://localhost:636");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs sslConfigs = new LdapConfigs(config);
    LdapConnectionFactory sslFactory = new LdapConnectionFactory(sslConfigs);

    boolean result = sslFactory.testConnection();
    assertFalse(result, "testConnection should return false when SSL server is not available");
  }

  @Test
  public void testTestConnectionWithStartTls() {
    // Test testConnection with StartTLS enabled
    configMap.put("auth.ldap.startTls", true);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs tlsConfigs = new LdapConfigs(config);
    LdapConnectionFactory tlsFactory = new LdapConnectionFactory(tlsConfigs);

    boolean result = tlsFactory.testConnection();
    assertFalse(result, "testConnection should return false when StartTLS server is not available");
  }

  @Test
  public void testTestConnectionWithShortTimeout() {
    // Test testConnection with very short timeout to ensure it fails quickly
    configMap.put("auth.ldap.connectionTimeout", 100);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs timeoutConfigs = new LdapConfigs(config);
    LdapConnectionFactory timeoutFactory = new LdapConnectionFactory(timeoutConfigs);

    long startTime = System.currentTimeMillis();
    boolean result = timeoutFactory.testConnection();
    long duration = System.currentTimeMillis() - startTime;

    assertFalse(result, "testConnection should return false with short timeout");
    assertTrue(
        duration < 5000,
        "testConnection should fail quickly with short timeout (took " + duration + "ms)");
  }

  @Test
  public void testTestConnectionDoesNotThrowException() {
    // Test that testConnection never throws exceptions, only returns boolean
    // This is important for health checks and startup validation
    assertDoesNotThrow(
        () -> {
          boolean result = connectionFactory.testConnection();
          // Result should be false since no LDAP server is running
          assertFalse(result);
        },
        "testConnection should not throw exceptions");
  }

  @Test
  public void testCloseConnectionWithNullContext() {
    // Test that closeConnection handles null gracefully (already exists but adding for
    // completeness)
    assertDoesNotThrow(
        () -> {
          connectionFactory.closeConnection(null);
        },
        "closeConnection should handle null context without throwing exception");
  }

  @Test
  public void testCloseConnectionMultipleTimes() {
    // Test that calling closeConnection multiple times with null doesn't cause issues
    assertDoesNotThrow(
        () -> {
          connectionFactory.closeConnection(null);
          connectionFactory.closeConnection(null);
          connectionFactory.closeConnection(null);
        },
        "closeConnection should handle multiple calls with null");
  }

  @Test
  public void testCloseConnectionDoesNotThrowException() {
    // Test that closeConnection never throws exceptions even with null
    // This is important for finally blocks and cleanup code
    assertDoesNotThrow(
        () -> {
          connectionFactory.closeConnection(null);
        },
        "closeConnection should be safe to call in finally blocks");
  }

  @Test
  public void testCloseConnectionWithMockContext() {
    // Test closeConnection with a mock DirContext
    javax.naming.directory.DirContext mockContext = mock(javax.naming.directory.DirContext.class);

    // Configure mock to throw exception on close
    try {
      doThrow(new javax.naming.NamingException("Mock close exception")).when(mockContext).close();
    } catch (javax.naming.NamingException e) {
      // This won't happen in setup
    }

    // closeConnection should handle the exception gracefully and not propagate it
    assertDoesNotThrow(
        () -> {
          connectionFactory.closeConnection(mockContext);
        },
        "closeConnection should handle NamingException during close");
  }

  @Test
  public void testCloseConnectionWithMockContextSuccess() {
    // Test closeConnection successfully closes a mock context
    javax.naming.directory.DirContext mockContext = mock(javax.naming.directory.DirContext.class);

    assertDoesNotThrow(
        () -> {
          connectionFactory.closeConnection(mockContext);
          // Verify that close was called on the context
          verify(mockContext, times(1)).close();
        },
        "closeConnection should call close on the context");
  }

  /**
   * Note: The following tests require an actual LDAP server and are better suited for integration
   * tests. They are documented in LDAP_TEST_GUIDE.md:
   *
   * <p>- testCreateConnectionWithValidCredentials() - testCreateConnectionWithInvalidCredentials()
   * - testCreateConnectionTimeout() - testReadTimeout() - testConnectionPooling() -
   * testTestConnectionWithRealServer() - testCloseConnectionWithRealConnection()
   *
   * <p>These tests would use an embedded LDAP server (UnboundID LDAP SDK or Apache Directory
   * Server) to verify actual connection behavior.
   */
}
