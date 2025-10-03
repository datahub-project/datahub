package security;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.Constructor;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import org.junit.jupiter.api.Test;

/**
 * Additional tests for AuthenticationManager focusing on callback handling and group extraction
 * functionality.
 */
public class AuthenticationManagerCallbackTest {

  // ========== Tests for WHZCallbackHandler ==========

  @Test
  public void testWHZCallbackHandlerWithNameAndPasswordCallbacks() throws Exception {
    // Use reflection to access the private inner class
    Class<?> callbackHandlerClass = getWHZCallbackHandlerClass();
    Constructor<?> constructor =
        callbackHandlerClass.getDeclaredConstructor(String.class, String.class);
    constructor.setAccessible(true);

    CallbackHandler handler = (CallbackHandler) constructor.newInstance("testuser", "testpass");

    NameCallback nameCallback = new NameCallback("Username:");
    PasswordCallback passwordCallback = new PasswordCallback("Password:", false);

    Callback[] callbacks = {nameCallback, passwordCallback};

    handler.handle(callbacks);

    assertEquals("testuser", nameCallback.getName());
    assertArrayEquals("testpass".toCharArray(), passwordCallback.getPassword());
  }

  @Test
  public void testWHZCallbackHandlerWithOnlyNameCallback() throws Exception {
    Class<?> callbackHandlerClass = getWHZCallbackHandlerClass();
    Constructor<?> constructor =
        callbackHandlerClass.getDeclaredConstructor(String.class, String.class);
    constructor.setAccessible(true);

    CallbackHandler handler = (CallbackHandler) constructor.newInstance("admin", "secret");

    NameCallback nameCallback = new NameCallback("Username:");
    Callback[] callbacks = {nameCallback};

    handler.handle(callbacks);

    assertEquals("admin", nameCallback.getName());
  }

  @Test
  public void testWHZCallbackHandlerWithOnlyPasswordCallback() throws Exception {
    Class<?> callbackHandlerClass = getWHZCallbackHandlerClass();
    Constructor<?> constructor =
        callbackHandlerClass.getDeclaredConstructor(String.class, String.class);
    constructor.setAccessible(true);

    CallbackHandler handler = (CallbackHandler) constructor.newInstance("user", "password123");

    PasswordCallback passwordCallback = new PasswordCallback("Password:", false);
    Callback[] callbacks = {passwordCallback};

    handler.handle(callbacks);

    assertArrayEquals("password123".toCharArray(), passwordCallback.getPassword());
  }

  @Test
  public void testWHZCallbackHandlerWithEmptyCallbacks() throws Exception {
    Class<?> callbackHandlerClass = getWHZCallbackHandlerClass();
    Constructor<?> constructor =
        callbackHandlerClass.getDeclaredConstructor(String.class, String.class);
    constructor.setAccessible(true);

    CallbackHandler handler = (CallbackHandler) constructor.newInstance("user", "pass");

    Callback[] callbacks = {};

    // Should not throw any exception
    assertDoesNotThrow(() -> handler.handle(callbacks));
  }

  @Test
  public void testWHZCallbackHandlerWithUnsupportedCallback() throws Exception {
    Class<?> callbackHandlerClass = getWHZCallbackHandlerClass();
    Constructor<?> constructor =
        callbackHandlerClass.getDeclaredConstructor(String.class, String.class);
    constructor.setAccessible(true);

    CallbackHandler handler = (CallbackHandler) constructor.newInstance("user", "pass");

    // Create a mock unsupported callback
    Callback unsupportedCallback = mock(Callback.class);
    Callback[] callbacks = {unsupportedCallback};

    // Should handle gracefully (the implementation ignores unsupported callbacks)
    assertDoesNotThrow(() -> handler.handle(callbacks));
  }

  @Test
  public void testWHZCallbackHandlerWithMixedCallbacks() throws Exception {
    Class<?> callbackHandlerClass = getWHZCallbackHandlerClass();
    Constructor<?> constructor =
        callbackHandlerClass.getDeclaredConstructor(String.class, String.class);
    constructor.setAccessible(true);

    CallbackHandler handler = (CallbackHandler) constructor.newInstance("mixeduser", "mixedpass");

    NameCallback nameCallback = new NameCallback("Username:");
    PasswordCallback passwordCallback = new PasswordCallback("Password:", false);
    Callback unsupportedCallback = mock(Callback.class);

    Callback[] callbacks = {nameCallback, unsupportedCallback, passwordCallback};

    handler.handle(callbacks);

    assertEquals("mixeduser", nameCallback.getName());
    assertArrayEquals("mixedpass".toCharArray(), passwordCallback.getPassword());
  }

  @Test
  public void testWHZCallbackHandlerWithNullUsername() throws Exception {
    Class<?> callbackHandlerClass = getWHZCallbackHandlerClass();
    Constructor<?> constructor =
        callbackHandlerClass.getDeclaredConstructor(String.class, String.class);
    constructor.setAccessible(true);

    CallbackHandler handler = (CallbackHandler) constructor.newInstance(null, "password");

    NameCallback nameCallback = new NameCallback("Username:");
    Callback[] callbacks = {nameCallback};

    handler.handle(callbacks);

    assertNull(nameCallback.getName());
  }

  @Test
  public void testWHZCallbackHandlerWithNullPassword() throws Exception {
    Class<?> callbackHandlerClass = getWHZCallbackHandlerClass();
    Constructor<?> constructor =
        callbackHandlerClass.getDeclaredConstructor(String.class, String.class);
    constructor.setAccessible(true);

    CallbackHandler handler = (CallbackHandler) constructor.newInstance("user", null);

    PasswordCallback passwordCallback = new PasswordCallback("Password:", false);
    Callback[] callbacks = {passwordCallback};

    // This should throw a NullPointerException since the implementation calls toCharArray() on null
    assertThrows(NullPointerException.class, () -> handler.handle(callbacks));
  }

  // ========== Tests for extractGroupsFromSubject (using reflection) ==========

  @Test
  public void testExtractGroupsFromSubjectWithGroupPrincipals() throws Exception {
    Subject subject = new Subject();

    // Add various types of principals
    TestGroupPrincipal groupPrincipal1 = new TestGroupPrincipal("Administrators");
    TestGroupPrincipal groupPrincipal2 = new TestGroupPrincipal("Users");
    TestRolePrincipal rolePrincipal = new TestRolePrincipal("Manager");
    TestPrincipal regularPrincipal = new TestPrincipal("john.doe");

    subject.getPrincipals().add(groupPrincipal1);
    subject.getPrincipals().add(groupPrincipal2);
    subject.getPrincipals().add(rolePrincipal);
    subject.getPrincipals().add(regularPrincipal);

    Set<String> groups = invokeExtractGroupsFromSubject(subject);

    assertNotNull(groups);
    assertEquals(3, groups.size()); // Should find 3 group/role principals
    assertTrue(groups.contains("Administrators"));
    assertTrue(groups.contains("Users"));
    assertTrue(groups.contains("Manager"));
    assertFalse(groups.contains("john.doe")); // Regular principal should not be included
  }

  @Test
  public void testExtractGroupsFromSubjectWithNoGroupPrincipals() throws Exception {
    Subject subject = new Subject();

    // Add only regular principals
    TestPrincipal principal1 = new TestPrincipal("user1");
    TestPrincipal principal2 = new TestPrincipal("user2");

    subject.getPrincipals().add(principal1);
    subject.getPrincipals().add(principal2);

    Set<String> groups = invokeExtractGroupsFromSubject(subject);

    assertNotNull(groups);
    assertTrue(groups.isEmpty());
  }

  @Test
  public void testExtractGroupsFromSubjectWithNullSubject() throws Exception {
    Set<String> groups = invokeExtractGroupsFromSubject(null);

    assertNotNull(groups);
    assertTrue(groups.isEmpty());
  }

  @Test
  public void testExtractGroupsFromSubjectWithEmptySubject() throws Exception {
    Subject subject = new Subject();

    Set<String> groups = invokeExtractGroupsFromSubject(subject);

    assertNotNull(groups);
    assertTrue(groups.isEmpty());
  }

  // ========== Helper Methods ==========

  private Class<?> getWHZCallbackHandlerClass() throws ClassNotFoundException {
    // Get the inner class
    Class<?>[] innerClasses = AuthenticationManager.class.getDeclaredClasses();
    for (Class<?> innerClass : innerClasses) {
      if (innerClass.getSimpleName().equals("WHZCallbackHandler")) {
        return innerClass;
      }
    }
    throw new ClassNotFoundException("WHZCallbackHandler inner class not found");
  }

  @SuppressWarnings("unchecked")
  private Set<String> invokeExtractGroupsFromSubject(Subject subject) throws Exception {
    // Use reflection to call the private static method
    java.lang.reflect.Method method =
        AuthenticationManager.class.getDeclaredMethod("extractGroupsFromSubject", Subject.class);
    method.setAccessible(true);
    return (Set<String>) method.invoke(null, subject);
  }

  // ========== Test Helper Classes ==========

  private static class TestPrincipal implements java.security.Principal {
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
      if (obj == null || getClass() != obj.getClass()) return false;
      TestPrincipal that = (TestPrincipal) obj;
      return java.util.Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(name);
    }
  }

  private static class TestGroupPrincipal implements java.security.Principal {
    private final String name;

    public TestGroupPrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      TestGroupPrincipal that = (TestGroupPrincipal) obj;
      return java.util.Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(name);
    }
  }

  private static class TestRolePrincipal implements java.security.Principal {
    private final String name;

    public TestRolePrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;
      TestRolePrincipal that = (TestRolePrincipal) obj;
      return java.util.Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return java.util.Objects.hash(name);
    }
  }
}
