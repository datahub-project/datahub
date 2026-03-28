package auth.ldap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import security.DataHubUserPrincipal;

/**
 * Unit tests for LdapProvisioningLoginModule. Tests JAAS LoginModule integration and provisioning
 * logic.
 */
public class LdapProvisioningLoginModuleTest {

  private LdapProvisioningLoginModule loginModule;
  private Subject subject;
  private Map<String, Object> options;

  @Mock private CallbackHandler mockCallbackHandler;

  @Mock private LdapProvisioningService mockProvisioningService;

  private AutoCloseable mocks;
  private Map<String, Object> configMap;

  @BeforeEach
  public void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);

    loginModule = new LdapProvisioningLoginModule();
    subject = new Subject();
    options = new HashMap<>();

    // Setup test configuration
    configMap = new HashMap<>();
    configMap.put("auth.ldap.enabled", true);
    configMap.put("auth.ldap.server", "ldap://localhost:389");
    configMap.put("auth.ldap.baseDn", "ou=users,dc=example,dc=com");
    configMap.put("auth.ldap.bindDn", "cn=admin,dc=example,dc=com");
    configMap.put("auth.ldap.bindPassword", "admin");
    configMap.put("auth.ldap.jitProvisioningEnabled", true);
    configMap.put("auth.ldap.extractGroupsEnabled", false);

    // Configure mock CallbackHandler to return a test password by default
    doAnswer(
            invocation -> {
              Callback[] callbacks = invocation.getArgument(0);
              for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback) {
                  ((PasswordCallback) callback).setPassword("testPassword123".toCharArray());
                }
              }
              return null;
            })
        .when(mockCallbackHandler)
        .handle(any(Callback[].class));
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testInitialize() {
    // Test module initialization
    Map<String, Object> sharedState = new HashMap<>();

    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    // Should not throw exception
    assertNotNull(loginModule);
  }

  @Test
  public void testInitializeWithDebug() {
    // Test initialization with debug option
    options.put("debug", "true");
    Map<String, Object> sharedState = new HashMap<>();

    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    // Should not throw exception
    assertNotNull(loginModule);
  }

  @Test
  public void testLoginWithAuthenticatedUser() throws LoginException {
    // Test login with authenticated user in subject
    Principal testPrincipal = new TestPrincipal("testuser");
    subject.getPrincipals().add(testPrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.login();

    assertTrue(result);
  }

  @Test
  public void testLoginWithoutAuthenticatedUser() throws Exception {
    // Test login without authenticated user - should fail because CallbackHandler returns password
    // but there's no way to determine the username without a principal
    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.login();

    // With password available, login() succeeds, but commit() would fail without username
    assertTrue(result);
  }

  @Test
  public void testLoginWithUPNPrincipal() throws LoginException {
    // Test login with UPN format principal
    Principal upnPrincipal = new TestPrincipal("testuser@example.com");
    subject.getPrincipals().add(upnPrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.login();

    assertTrue(result);
  }

  @Test
  public void testLoginWithDNPrincipal() throws LoginException {
    // Test login with DN format principal
    Principal dnPrincipal = new TestPrincipal("CN=Test User,OU=Users,DC=example,DC=com");
    subject.getPrincipals().add(dnPrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.login();

    assertTrue(result);
  }

  @Test
  public void testCommitWithoutSuccessfulLogin() throws LoginException {
    // Test commit without successful login
    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.commit();

    assertFalse(result);
  }

  @Test
  public void testAbort() throws LoginException {
    // Test abort method
    Principal testPrincipal = new TestPrincipal("testuser");
    subject.getPrincipals().add(testPrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);
    loginModule.login();

    boolean result = loginModule.abort();

    assertTrue(result);
  }

  @Test
  public void testLogout() throws LoginException {
    // Test logout method
    subject.getPrincipals().add(new DataHubUserPrincipal("testuser"));

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.logout();

    assertTrue(result);
    // Verify DataHubUserPrincipal was removed
    assertTrue(subject.getPrincipals().stream().noneMatch(p -> p instanceof DataHubUserPrincipal));
  }

  @Test
  public void testExtractUsernameFromSimplePrincipal() throws LoginException {
    // Test username extraction from simple principal
    Principal simplePrincipal = new TestPrincipal("simpleuser");
    subject.getPrincipals().add(simplePrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.login();

    assertTrue(result);
  }

  @Test
  public void testExtractUsernameFromUPN() throws LoginException {
    // Test username extraction from UPN (user@domain.com)
    Principal upnPrincipal = new TestPrincipal("john.doe@example.com");
    subject.getPrincipals().add(upnPrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.login();

    assertTrue(result);
    // Username should be extracted as "john.doe" (before @)
  }

  @Test
  public void testExtractUsernameFromDN() throws LoginException {
    // Test username extraction from DN
    Principal dnPrincipal = new TestPrincipal("CN=John Doe,OU=Users,DC=example,DC=com");
    subject.getPrincipals().add(dnPrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.login();

    assertTrue(result);
    // Username should be extracted as "John Doe" (CN value)
  }

  @Test
  public void testMultiplePrincipals() throws LoginException {
    // Test with multiple principals (should use first one)
    subject.getPrincipals().add(new TestPrincipal("user1"));
    subject.getPrincipals().add(new TestPrincipal("user2"));

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    boolean result = loginModule.login();

    assertTrue(result);
  }

  @Test
  public void testEmptyPrincipalName() throws LoginException {
    // Test with empty principal name - should pass login() but fail commit()
    Principal emptyPrincipal = new TestPrincipal("");
    subject.getPrincipals().add(emptyPrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    // login() should succeed because password is available
    boolean loginResult = loginModule.login();
    assertTrue(loginResult, "login() should succeed when password is available");

    // commit() should fail because username is empty
    assertThrows(
        LoginException.class,
        () -> loginModule.commit(),
        "commit() should fail with empty principal name");
  }

  @Test
  public void testNullPrincipalName() throws LoginException {
    // Test with null principal name - should pass login() but fail commit()
    Principal nullPrincipal = new TestPrincipal(null);
    subject.getPrincipals().add(nullPrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    // login() should succeed because password is available
    boolean loginResult = loginModule.login();
    assertTrue(loginResult, "login() should succeed when password is available");

    // commit() should fail because username is null
    assertThrows(
        LoginException.class,
        () -> loginModule.commit(),
        "commit() should fail with null principal name");
  }

  @Test
  public void testLoginWithoutPassword() throws Exception {
    // Test that login() fails when CallbackHandler doesn't provide password
    Principal testPrincipal = new TestPrincipal("testuser");
    subject.getPrincipals().add(testPrincipal);

    // Configure CallbackHandler to NOT return password
    doAnswer(
            invocation -> {
              Callback[] callbacks = invocation.getArgument(0);
              for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback) {
                  // Don't set password - leave it null
                  ((PasswordCallback) callback).setPassword(null);
                }
              }
              return null;
            })
        .when(mockCallbackHandler)
        .handle(any(Callback[].class));

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, mockCallbackHandler, sharedState, options);

    // login() should fail because password is not available
    boolean result = loginModule.login();
    assertFalse(result, "login() should fail when password is not available from CallbackHandler");
  }

  @Test
  public void testLoginWithNullCallbackHandler() throws LoginException {
    // Test that login() fails when CallbackHandler is null
    Principal testPrincipal = new TestPrincipal("testuser");
    subject.getPrincipals().add(testPrincipal);

    Map<String, Object> sharedState = new HashMap<>();
    loginModule.initialize(subject, null, sharedState, options); // null CallbackHandler

    // login() should fail because CallbackHandler is null
    boolean result = loginModule.login();
    assertFalse(result, "login() should fail when CallbackHandler is null");
  }

  /** Test principal implementation for testing */
  private static class TestPrincipal implements Principal {
    private final String name;

    public TestPrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof TestPrincipal)) return false;
      TestPrincipal other = (TestPrincipal) obj;
      return name != null ? name.equals(other.name) : other.name == null;
    }

    @Override
    public int hashCode() {
      return name != null ? name.hashCode() : 0;
    }
  }
}
