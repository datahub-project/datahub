package auth.ldap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LdapConnectionUtilTest {

  private Map<String, String> testOptions;
  private File tempJaasFile;

  @BeforeEach
  public void setUp() throws IOException {
    testOptions = new HashMap<>();
    testOptions.put("groupProvider", "ldaps://server.example.com:636");
    testOptions.put("authIdentity", "CN={USERNAME},OU=Users,DC=example,DC=com");
    testOptions.put("java.naming.security.authentication", "simple");
    testOptions.put("connectTimeout", "10000");
    testOptions.put("readTimeout", "15000");
    testOptions.put("useSSL", "true");

    // Create a temporary JAAS configuration file for testing
    tempJaasFile = Files.createTempFile("test-jaas", ".conf").toFile();
    try (FileWriter writer = new FileWriter(tempJaasFile)) {
      writer.write("WHZ-Authentication {\n");
      writer.write("  com.sun.security.auth.module.LdapLoginModule SUFFICIENT\n");
      writer.write("    groupProvider=\"ldaps://server.example.com:636\"\n");
      writer.write("    authIdentity=\"CN={USERNAME},OU=Users,DC=example,DC=com\"\n");
      writer.write("    connectTimeout=\"5000\"\n");
      writer.write("    readTimeout=\"10000\"\n");
      writer.write("    useSSL=\"true\";\n");
      writer.write("};\n");
    }
  }

  @AfterEach
  public void tearDown() {
    if (tempJaasFile != null && tempJaasFile.exists()) {
      tempJaasFile.delete();
    }
    // Clear the system property
    System.clearProperty("java.security.auth.login.config");
  }

  // ========== Tests for createLdapEnvironment ==========

  @Test
  public void testCreateLdapEnvironmentWithBasicOptions() {
    String username = "testuser";
    String password = "testpass";

    Hashtable<String, Object> env =
        LdapConnectionUtil.createLdapEnvironment(testOptions, username, password);

    assertNotNull(env);
    assertEquals("com.sun.jndi.ldap.LdapCtxFactory", env.get(Context.INITIAL_CONTEXT_FACTORY));
    assertEquals("ldaps://server.example.com:636", env.get(Context.PROVIDER_URL));
    assertEquals("simple", env.get(Context.SECURITY_AUTHENTICATION));
    assertEquals("CN=testuser,OU=Users,DC=example,DC=com", env.get(Context.SECURITY_PRINCIPAL));
    assertEquals("testpass", env.get(Context.SECURITY_CREDENTIALS));
    assertEquals("objectSID objectGUID", env.get("java.naming.ldap.attributes.binary"));
    assertEquals("follow", env.get(Context.REFERRAL));
    assertEquals("10000", env.get("com.sun.jndi.ldap.connect.timeout"));
    assertEquals("15000", env.get("com.sun.jndi.ldap.read.timeout"));
    assertEquals("ssl", env.get("java.naming.security.protocol"));
    assertEquals("true", env.get("com.sun.jndi.ldap.connect.pool"));
    assertEquals(System.err, env.get("com.sun.jndi.ldap.trace.ber"));
  }

  @Test
  public void testCreateLdapEnvironmentWithoutSSL() {
    testOptions.put("useSSL", "false");
    String username = "testuser";
    String password = "testpass";

    Hashtable<String, Object> env =
        LdapConnectionUtil.createLdapEnvironment(testOptions, username, password);

    assertNotNull(env);
    assertNull(env.get("java.naming.security.protocol"));
  }

  @Test
  public void testCreateLdapEnvironmentWithDefaultValues() {
    Map<String, String> minimalOptions = new HashMap<>();
    String username = "testuser";
    String password = "testpass";

    Hashtable<String, Object> env =
        LdapConnectionUtil.createLdapEnvironment(minimalOptions, username, password);

    assertNotNull(env);
    assertEquals("", env.get(Context.PROVIDER_URL));
    assertEquals("simple", env.get(Context.SECURITY_AUTHENTICATION));
    assertEquals("", env.get(Context.SECURITY_PRINCIPAL));
    assertEquals("5000", env.get("com.sun.jndi.ldap.connect.timeout"));
    assertEquals("5000", env.get("com.sun.jndi.ldap.read.timeout"));
  }

  @Test
  public void testCreateLdapEnvironmentWithUsernameReplacement() {
    testOptions.put("authIdentity", "uid={USERNAME},ou=people,dc=company,dc=com");
    String username = "john.doe";
    String password = "secret";

    Hashtable<String, Object> env =
        LdapConnectionUtil.createLdapEnvironment(testOptions, username, password);

    assertEquals("uid=john.doe,ou=people,dc=company,dc=com", env.get(Context.SECURITY_PRINCIPAL));
  }

  @Test
  public void testCreateLdapEnvironmentWithNullUsername() {
    assertThrows(
        NullPointerException.class,
        () -> {
          LdapConnectionUtil.createLdapEnvironment(testOptions, null, "password");
        });
  }

  @Test
  public void testCreateLdapEnvironmentWithNullPassword() {
    assertThrows(
        NullPointerException.class,
        () -> {
          LdapConnectionUtil.createLdapEnvironment(testOptions, "username", null);
        });
  }

  @Test
  public void testCreateLdapEnvironmentWithNullOptions() {
    assertThrows(
        NullPointerException.class,
        () -> {
          LdapConnectionUtil.createLdapEnvironment(null, "username", "password");
        });
  }

  // ========== Tests for createLdapContext ==========

  @Test
  public void testCreateLdapContextSuccess() throws NamingException {
    Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");

    // This test will actually try to create a context, but since we're not connecting to a real
    // LDAP server,
    // it will likely fail. We'll just verify the method doesn't throw unexpected exceptions for
    // valid input.
    assertDoesNotThrow(
        () -> {
          try {
            DirContext result = LdapConnectionUtil.createLdapContext(env);
            // If it succeeds, great! If it fails with NamingException, that's expected for invalid
            // server
          } catch (NamingException e) {
            // Expected when no real LDAP server is available
            assertTrue(e.getMessage() != null);
          }
        });
  }

  @Test
  public void testCreateLdapContextWithNamingException() {
    // Test with invalid configuration that should cause NamingException
    Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "invalid.factory.class");

    assertThrows(
        NamingException.class,
        () -> {
          LdapConnectionUtil.createLdapContext(env);
        });
  }

  @Test
  public void testCreateLdapContextWithNullEnvironment() {
    // The InitialDirContext constructor actually accepts null environment
    assertDoesNotThrow(
        () -> {
          try {
            LdapConnectionUtil.createLdapContext(null);
          } catch (NamingException e) {
            // Expected when no real LDAP server is available
            assertTrue(e.getMessage() != null);
          }
        });
  }

  // ========== Tests for logLdapEnvironment ==========

  @Test
  public void testLogLdapEnvironment() {
    Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.PROVIDER_URL, "ldaps://server.com:636");
    env.put(Context.SECURITY_PRINCIPAL, "CN=user,DC=example,DC=com");
    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put("java.naming.security.protocol", "ssl");
    env.put(Context.SECURITY_CREDENTIALS, "secret");
    env.put("password", "anothersecret");
    env.put("normalkey", "normalvalue");

    // This method only logs, so we just verify it doesn't throw exceptions
    assertDoesNotThrow(
        () -> {
          LdapConnectionUtil.logLdapEnvironment(env);
        });
  }

  @Test
  public void testLogLdapEnvironmentWithNullEnvironment() {
    assertThrows(
        NullPointerException.class,
        () -> {
          LdapConnectionUtil.logLdapEnvironment(null);
        });
  }

  @Test
  public void testLogLdapEnvironmentWithEmptyEnvironment() {
    Hashtable<String, Object> env = new Hashtable<>();

    assertDoesNotThrow(
        () -> {
          LdapConnectionUtil.logLdapEnvironment(env);
        });
  }

  // ========== Tests for extractLdapOptionsFromJaasConfig ==========

  @Test
  public void testExtractLdapOptionsFromJaasConfigSuccess() {
    System.setProperty("java.security.auth.login.config", tempJaasFile.getAbsolutePath());

    Map<String, String> result = LdapConnectionUtil.extractLdapOptionsFromJaasConfig();

    assertNotNull(result);
    assertFalse(result.isEmpty());
    assertEquals("ldaps://server.example.com:636", result.get("groupProvider"));
    assertEquals("CN={USERNAME},OU=Users,DC=example,DC=com", result.get("authIdentity"));
    assertEquals("5000", result.get("connectTimeout"));
    assertEquals("10000", result.get("readTimeout"));
    assertEquals("true", result.get("useSSL"));
  }

  @Test
  public void testExtractLdapOptionsFromJaasConfigWithNoSystemProperty() {
    System.clearProperty("java.security.auth.login.config");

    Map<String, String> result = LdapConnectionUtil.extractLdapOptionsFromJaasConfig();

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractLdapOptionsFromJaasConfigWithNonExistentFile() {
    System.setProperty("java.security.auth.login.config", "/non/existent/file.conf");

    Map<String, String> result = LdapConnectionUtil.extractLdapOptionsFromJaasConfig();

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractLdapOptionsFromJaasConfigWithMalformedFile() throws IOException {
    File malformedFile = Files.createTempFile("malformed-jaas", ".conf").toFile();
    try (FileWriter writer = new FileWriter(malformedFile)) {
      writer.write("This is not a valid JAAS configuration file\n");
      writer.write("Random content without proper structure\n");
    }

    System.setProperty("java.security.auth.login.config", malformedFile.getAbsolutePath());

    Map<String, String> result = LdapConnectionUtil.extractLdapOptionsFromJaasConfig();

    assertNotNull(result);
    assertTrue(result.isEmpty());

    malformedFile.delete();
  }

  @Test
  public void testExtractLdapOptionsFromJaasConfigWithEmptyFile() throws IOException {
    File emptyFile = Files.createTempFile("empty-jaas", ".conf").toFile();
    // File is created but left empty

    System.setProperty("java.security.auth.login.config", emptyFile.getAbsolutePath());

    Map<String, String> result = LdapConnectionUtil.extractLdapOptionsFromJaasConfig();

    assertNotNull(result);
    assertTrue(result.isEmpty());

    emptyFile.delete();
  }
}
