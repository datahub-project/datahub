package security;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PropertyFileLoginModuleTest {

  private PropertyFileLoginModule loginModule;
  private Path tempFilePath;
  private File tempPropsFile;

  @BeforeEach
  public void setUp() throws IOException {
    loginModule = new PropertyFileLoginModule();

    // Create a temporary properties file for testing
    tempFilePath = Files.createTempFile("test-users", ".props");
    tempPropsFile = tempFilePath.toFile();

    // Write test users to the file
    try (FileWriter writer = new FileWriter(tempPropsFile)) {
      writer.write("testuser:testpassword\n");
      writer.write("datahub:datahub\n");
      writer.write("admin:admin123\n");
    }
  }

  @AfterEach
  public void tearDown() {
    // Clean up the temporary file
    if (tempPropsFile != null && tempPropsFile.exists()) {
      tempPropsFile.delete();
    }
  }

  @Test
  public void testSuccessfulAuthentication() throws LoginException {
    // Set up the login module with necessary options
    Subject subject = new Subject();
    TestCallbackHandler callbackHandler = new TestCallbackHandler("datahub", "datahub");
    Map<String, Object> options = new HashMap<>();
    options.put("debug", "true");
    options.put("file", tempPropsFile.getAbsolutePath());

    loginModule.initialize(subject, callbackHandler, null, options);

    // Perform login
    boolean loginResult = loginModule.login();
    assertTrue(loginResult, "Login should succeed with correct credentials");

    // Commit the authentication
    boolean commitResult = loginModule.commit();
    assertTrue(commitResult, "Commit should succeed after successful login");

    // Verify principal was added to the subject
    assertEquals(1, subject.getPrincipals().size(), "Subject should have one principal added");
    assertTrue(
        subject.getPrincipals().stream().anyMatch(p -> p.getName().equals("datahub")),
        "Subject should have the correct principal");
  }

  @Test
  public void testFailedAuthentication() throws LoginException {
    // Set up the login module with necessary options
    Subject subject = new Subject();
    TestCallbackHandler callbackHandler = new TestCallbackHandler("datahub", "wrongpassword");
    Map<String, Object> options = new HashMap<>();
    options.put("debug", "true");
    options.put("file", tempPropsFile.getAbsolutePath());

    loginModule.initialize(subject, callbackHandler, null, options);

    // Perform login
    boolean loginResult = loginModule.login();
    assertFalse(loginResult, "Login should fail with incorrect credentials");

    // Commit should return false after failed login
    boolean commitResult = loginModule.commit();
    assertFalse(commitResult, "Commit should fail after unsuccessful login");

    // Verify no principals were added
    assertEquals(
        0, subject.getPrincipals().size(), "Subject should have no principals after failed login");
  }

  @Test
  public void testNonexistentUser() throws LoginException {
    // Set up the login module with necessary options
    Subject subject = new Subject();
    TestCallbackHandler callbackHandler = new TestCallbackHandler("nonexistentuser", "password");
    Map<String, Object> options = new HashMap<>();
    options.put("debug", "true");
    options.put("file", tempPropsFile.getAbsolutePath());

    loginModule.initialize(subject, callbackHandler, null, options);

    // Perform login
    boolean loginResult = loginModule.login();
    assertFalse(loginResult, "Login should fail with nonexistent user");
  }

  @Test
  public void testNonexistentFile() throws LoginException {
    // Set up the login module with a nonexistent file
    Subject subject = new Subject();
    TestCallbackHandler callbackHandler = new TestCallbackHandler("datahub", "datahub");
    Map<String, Object> options = new HashMap<>();
    options.put("debug", "true");
    options.put("file", "/nonexistent/file.props");

    loginModule.initialize(subject, callbackHandler, null, options);

    // Perform login
    boolean loginResult = loginModule.login();
    assertFalse(loginResult, "Login should fail with nonexistent file");
  }

  @Test
  public void testLogoutClearsCredentials() throws LoginException {
    // Set up and perform successful login
    Subject subject = new Subject();
    TestCallbackHandler callbackHandler = new TestCallbackHandler("admin", "admin123");
    Map<String, Object> options = new HashMap<>();
    options.put("debug", "true");
    options.put("file", tempPropsFile.getAbsolutePath());

    loginModule.initialize(subject, callbackHandler, null, options);
    loginModule.login();
    loginModule.commit();

    // Verify principal was added
    assertEquals(1, subject.getPrincipals().size(), "Subject should have one principal added");

    // Perform logout
    boolean logoutResult = loginModule.logout();
    assertTrue(logoutResult, "Logout should succeed");

    // Verify principal was removed
    assertEquals(
        0, subject.getPrincipals().size(), "Subject should have no principals after logout");
  }

  /** Simple test callback handler that provides fixed username and password. */
  private static class TestCallbackHandler implements CallbackHandler {
    private final String username;
    private final String password;

    public TestCallbackHandler(String username, String password) {
      this.username = username;
      this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          ((NameCallback) callback).setName(username);
        } else if (callback instanceof PasswordCallback) {
          ((PasswordCallback) callback).setPassword(password.toCharArray());
        }
      }
    }
  }
}
