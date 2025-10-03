package security;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import javax.naming.AuthenticationException;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthenticationManagerTest {

  private File tempPropsFile;
  private TestJaasConfiguration jaasConfig;
  private static Configuration originalConfig;

  @BeforeAll
  public static void setUpClass() {
    // Save the original JAAS configuration
    originalConfig = Configuration.getConfiguration();
  }

  @BeforeEach
  public void setUp() throws IOException {
    // Create a temporary properties file for testing
    tempPropsFile = Files.createTempFile("test-users", ".props").toFile();

    // Write test users to the file
    try (FileWriter writer = new FileWriter(tempPropsFile)) {
      writer.write("testuser:testpassword\n");
      writer.write("datahub:datahub\n");
      writer.write("admin:admin123\n");
    }

    // Set up a test JAAS configuration - use the fully qualified name of our custom login module
    jaasConfig = new TestJaasConfiguration();
    // We need to use the actual class that's available in the test classpath
    jaasConfig.setLoginModuleClass(PropertyFileLoginModule.class.getName());
    jaasConfig.setOption("file", tempPropsFile.getAbsolutePath());
    jaasConfig.setOption("debug", "true");

    // Install the test configuration
    Configuration.setConfiguration(jaasConfig);

    // Verify our configuration was properly applied
    AppConfigurationEntry[] entries =
        Configuration.getConfiguration().getAppConfigurationEntry("WHZ-Authentication");
    assertNotNull(entries, "JAAS configuration should be applied");
    assertEquals(1, entries.length, "Should have one login module configured");
    assertEquals(
        PropertyFileLoginModule.class.getName(),
        entries[0].getLoginModuleName(),
        "Login module class should match PropertyFileLoginModule");
  }

  @AfterEach
  public void tearDown() {
    // Restore the original JAAS configuration
    Configuration.setConfiguration(originalConfig);

    // Clean up the temporary file
    if (tempPropsFile != null && tempPropsFile.exists()) {
      tempPropsFile.delete();
    }
  }

  // Since we can't easily mock static methods without mockito-inline,
  // we'll test the validation logic directly and try a real integration test

  @Test
  public void testEmptyUsername() {
    // Test with empty username
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AuthenticationManager.authenticateAndGetGroupsAndSubject("", "password"),
            "Should throw IllegalArgumentException for empty username");

    assertEquals("Username cannot be empty", exception.getMessage());
  }

  @Test
  public void testNullUsername() {
    // Test with null username
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AuthenticationManager.authenticateAndGetGroupsAndSubject(null, "password"),
            "Should throw IllegalArgumentException for null username");

    assertEquals("Username cannot be empty", exception.getMessage());
  }

  /**
   * Integration test that actually performs authentication against the custom login module. This
   * test will be skipped if the PropertyFileLoginModule class is not available.
   */
  @Test
  public void testRealAuthentication() {
    // Test successful authentication
    try {
      AuthenticationManager.AuthResult authResult =
          AuthenticationManager.authenticateAndGetGroupsAndSubject("datahub", "datahub");
      Subject subject = authResult.subject;
      // Verify the Subject is returned and contains expected data
      assertNotNull(subject, "Subject should not be null for valid credentials");
      assertFalse(subject.getPrincipals().isEmpty(), "Subject should contain principals");

      // Verify principal contains the username
      boolean foundUserPrincipal =
          subject.getPrincipals().stream().anyMatch(p -> p.getName().contains("datahub"));
      assertTrue(foundUserPrincipal, "Subject should contain a principal with the username");

    } catch (Exception e) {
      fail("Authentication should succeed with valid credentials: " + e.getMessage());
    }
  }

  @Test
  public void testInvalidCredentials() {
    // Test failed authentication
    Exception exception =
        assertThrows(
            AuthenticationException.class,
            () ->
                AuthenticationManager.authenticateAndGetGroupsAndSubject(
                    "datahub", "wrongpassword"),
            "Should throw AuthenticationException for invalid credentials");

    // Make sure we get a login failure message
    assertTrue(
        exception.getMessage() != null && !exception.getMessage().isEmpty(),
        "Exception message should not be empty");
  }

  @Test
  public void testEmptyPassword() {
    // Test authentication with empty password
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AuthenticationManager.authenticateAndGetGroupsAndSubject("testuser", ""),
            "Should throw IllegalArgumentException for empty username");

    assertEquals("Password cannot be empty", exception.getMessage());
  }

  @Test
  public void testNullPassword() {
    // Test authentication with null password
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AuthenticationManager.authenticateAndGetGroupsAndSubject("testuser", null),
            "Should throw IllegalArgumentException for empty username");

    assertEquals("Password cannot be empty", exception.getMessage());
  }

  @Test
  public void testMultipleValidUsers() throws Exception {
    // Test different users return different subjects
    AuthenticationManager.AuthResult authResult1 =
        AuthenticationManager.authenticateAndGetGroupsAndSubject("testuser", "testpassword");
    Subject subject1 = authResult1.subject;
    AuthenticationManager.AuthResult authResult2 =
        AuthenticationManager.authenticateAndGetGroupsAndSubject("datahub", "datahub");
    Subject subject2 = authResult2.subject;
    AuthenticationManager.AuthResult authResult3 =
        AuthenticationManager.authenticateAndGetGroupsAndSubject("admin", "admin123");
    Subject subject3 = authResult3.subject;

    assertNotNull(subject1, "Subject1 should not be null");
    assertNotNull(subject2, "Subject2 should not be null");
    assertNotNull(subject3, "Subject3 should not be null");

    // Subjects should be different instances
    assertNotSame(subject1, subject2, "Subjects should be different instances");
    assertNotSame(subject2, subject3, "Subjects should be different instances");
  }

  /** Method used by the @EnabledIf annotation to conditionally enable the integration test. */
  boolean isPropertyFileLoginModuleAvailable() {
    try {
      Class.forName(PropertyFileLoginModule.class.getName());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /** Simple test JAAS configuration that can be programmatically configured. */
  private static class TestJaasConfiguration extends Configuration {
    private String loginModuleClass;
    private final Map<String, String> options = new HashMap<>();

    public void setLoginModuleClass(String loginModuleClass) {
      this.loginModuleClass = loginModuleClass;
    }

    public void setOption(String key, String value) {
      options.put(key, value);
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      if ("WHZ-Authentication".equals(name) && loginModuleClass != null) {
        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(
              loginModuleClass, AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT, options)
        };
      }
      return null;
    }
  }
}
