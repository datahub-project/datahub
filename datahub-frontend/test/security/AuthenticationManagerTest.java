package security;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import auth.ldap.LdapConnectionUtil;
import auth.ldap.LdapUserAttributeExtractor;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import javax.naming.AuthenticationException;
import javax.naming.CommunicationException;
import javax.naming.NamingEnumeration;
import javax.naming.NoPermissionException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchResult;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

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

    // Set up a test JAAS configuration
    jaasConfig = new TestJaasConfiguration();
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

  // ========== Tests for authenticateAndGetGroupsAndSubject ==========

  @Test
  public void testEmptyUsername() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AuthenticationManager.authenticateAndGetGroupsAndSubject("", "password"),
            "Should throw IllegalArgumentException for empty username");

    assertEquals("Username cannot be empty", exception.getMessage());
  }

  @Test
  public void testNullUsername() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AuthenticationManager.authenticateAndGetGroupsAndSubject(null, "password"),
            "Should throw IllegalArgumentException for null username");

    assertEquals("Username cannot be empty", exception.getMessage());
  }

  @Test
  public void testEmptyPassword() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AuthenticationManager.authenticateAndGetGroupsAndSubject("testuser", ""),
            "Should throw IllegalArgumentException for empty password");

    assertEquals("Password cannot be empty", exception.getMessage());
  }

  @Test
  public void testNullPassword() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> AuthenticationManager.authenticateAndGetGroupsAndSubject("testuser", null),
            "Should throw IllegalArgumentException for null password");

    assertEquals("Password cannot be empty", exception.getMessage());
  }

  @Test
  public void testWhitespaceOnlyUsername() {
    // Test with various whitespace-only usernames
    String[] whitespaceUsernames = {"   ", "\t", "\n", " \t \n "};

    for (String username : whitespaceUsernames) {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> AuthenticationManager.authenticateAndGetGroupsAndSubject(username, "password"),
              "Should throw IllegalArgumentException for whitespace-only username: '"
                  + username
                  + "'");

      assertEquals("Username cannot be empty", exception.getMessage());
    }
  }

  @Test
  public void testWhitespaceOnlyPassword() {
    // Test with various whitespace-only passwords
    String[] whitespacePasswords = {"   ", "\t", "\n", " \t \n "};

    for (String password : whitespacePasswords) {
      IllegalArgumentException exception =
          assertThrows(
              IllegalArgumentException.class,
              () -> AuthenticationManager.authenticateAndGetGroupsAndSubject("testuser", password),
              "Should throw IllegalArgumentException for whitespace-only password: '"
                  + password
                  + "'");

      assertEquals("Password cannot be empty", exception.getMessage());
    }
  }

  @Test
  public void testRealAuthentication() {
    try {
      AuthenticationManager.AuthResult authResult =
          AuthenticationManager.authenticateAndGetGroupsAndSubject("datahub", "datahub");
      Subject subject = authResult.subject;

      assertNotNull(subject, "Subject should not be null for valid credentials");
      assertFalse(subject.getPrincipals().isEmpty(), "Subject should contain principals");
      assertNotNull(authResult.groups, "Groups should not be null");
      assertNotNull(authResult.ldapOptions, "LDAP options should not be null");

      boolean foundUserPrincipal =
          subject.getPrincipals().stream().anyMatch(p -> p.getName().contains("datahub"));
      assertTrue(foundUserPrincipal, "Subject should contain a principal with the username");

    } catch (Exception e) {
      fail("Authentication should succeed with valid credentials: " + e.getMessage());
    }
  }

  @Test
  public void testInvalidCredentials() {
    Exception exception =
        assertThrows(
            AuthenticationException.class,
            () ->
                AuthenticationManager.authenticateAndGetGroupsAndSubject(
                    "datahub", "wrongpassword"),
            "Should throw AuthenticationException for invalid credentials");

    assertTrue(
        exception.getMessage() != null && !exception.getMessage().isEmpty(),
        "Exception message should not be empty");
  }

  @Test
  public void testMultipleValidUsers() throws Exception {
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

    assertNotSame(subject1, subject2, "Subjects should be different instances");
    assertNotSame(subject2, subject3, "Subjects should be different instances");
  }

  // ========== Tests for extractBaseDN ==========

  @Test
  public void testExtractBaseDNFromValidUrl() {
    String ldapUrl = "ldaps://server.example.com:636/DC=EXAMPLE,DC=COM";
    String result = AuthenticationManager.extractBaseDN(ldapUrl);
    assertEquals("DC=EXAMPLE,DC=COM", result);
  }

  @Test
  public void testExtractBaseDNFromUrlWithoutPort() {
    String ldapUrl = "ldap://server.example.com/CN=Users,DC=DOMAIN,DC=LOCAL";
    String result = AuthenticationManager.extractBaseDN(ldapUrl);
    assertEquals("CN=Users,DC=DOMAIN,DC=LOCAL", result);
  }

  @Test
  public void testExtractBaseDNFromUrlWithComplexDN() {
    String ldapUrl = "ldaps://ad.company.com:636/OU=People,OU=Corporate,DC=company,DC=com";
    String result = AuthenticationManager.extractBaseDN(ldapUrl);
    assertEquals("OU=People,OU=Corporate,DC=company,DC=com", result);
  }

  @Test
  public void testExtractBaseDNFromUrlWithoutBaseDN() {
    String ldapUrl = "ldaps://server.example.com:636/";
    String result = AuthenticationManager.extractBaseDN(ldapUrl);
    assertEquals("", result);
  }

  @Test
  public void testExtractBaseDNFromUrlWithoutSlash() {
    String ldapUrl = "ldaps://server.example.com:636";
    String result = AuthenticationManager.extractBaseDN(ldapUrl);
    assertEquals("", result);
  }

  @Test
  public void testExtractBaseDNFromNullUrl() {
    String result = AuthenticationManager.extractBaseDN(null);
    assertEquals("", result);
  }

  @Test
  public void testExtractBaseDNFromEmptyUrl() {
    String result = AuthenticationManager.extractBaseDN("");
    assertEquals("", result);
  }

  // ========== Tests for extractUserDN ==========

  @Test
  public void testExtractUserDNFromNullSubject() {
    String result = AuthenticationManager.extractUserDN(null);
    assertNull(result);
  }

  @Test
  public void testExtractUserDNFromEmptySubject() {
    Subject subject = new Subject();
    String result = AuthenticationManager.extractUserDN(subject);
    assertNull(result);
  }

  @Test
  public void testExtractUserDNFromSubjectWithValidDN() {
    Subject subject = new Subject();
    TestPrincipal dnPrincipal = new TestPrincipal("CN=John Doe,OU=Users,DC=example,DC=com");
    TestPrincipal otherPrincipal = new TestPrincipal("regularuser");
    subject.getPrincipals().add(dnPrincipal);
    subject.getPrincipals().add(otherPrincipal);

    String result = AuthenticationManager.extractUserDN(subject);
    assertEquals("CN=John Doe,OU=Users,DC=example,DC=com", result);
  }

  @Test
  public void testExtractUserDNFromSubjectWithoutDN() {
    Subject subject = new Subject();
    TestPrincipal regularPrincipal = new TestPrincipal("regularuser");
    TestPrincipal anotherPrincipal = new TestPrincipal("anotheruser");
    subject.getPrincipals().add(regularPrincipal);
    subject.getPrincipals().add(anotherPrincipal);

    String result = AuthenticationManager.extractUserDN(subject);
    assertNull(result);
  }

  @Test
  public void testExtractUserDNFromSubjectWithMultipleDNs() {
    Subject subject = new Subject();
    TestPrincipal dnPrincipal1 = new TestPrincipal("CN=John Doe,OU=Users,DC=example,DC=com");
    TestPrincipal dnPrincipal2 = new TestPrincipal("CN=Jane Smith,OU=Admins,DC=example,DC=com");
    subject.getPrincipals().add(dnPrincipal1);
    subject.getPrincipals().add(dnPrincipal2);

    String result = AuthenticationManager.extractUserDN(subject);
    // Should return the first DN found
    assertTrue(
        result.equals("CN=John Doe,OU=Users,DC=example,DC=com")
            || result.equals("CN=Jane Smith,OU=Admins,DC=example,DC=com"));
  }

  // ========== Tests for queryUserGroups ==========

  @Test
  public void testQueryUserGroupsWithNullUserDN() {
    Map<String, String> options = new HashMap<>();
    options.put("userProvider", "ldaps://server.com:636/DC=EXAMPLE,DC=COM");

    assertThrows(
        NullPointerException.class,
        () -> {
          AuthenticationManager.queryUserGroups(null, options, "testuser", "password");
        });
  }

  @Test
  public void testQueryUserGroupsWithNullOptions() {
    assertThrows(
        NullPointerException.class,
        () -> {
          AuthenticationManager.queryUserGroups(
              "CN=Test,DC=example,DC=com", null, "testuser", "password");
        });
  }

  @Test
  public void testQueryUserGroupsWithNullUsername() {
    Map<String, String> options = new HashMap<>();
    options.put("userProvider", "ldaps://server.com:636/DC=EXAMPLE,DC=COM");

    assertThrows(
        NullPointerException.class,
        () -> {
          AuthenticationManager.queryUserGroups(
              "CN=Test,DC=example,DC=com", options, null, "password");
        });
  }

  @Test
  public void testQueryUserGroupsWithNullPassword() {
    Map<String, String> options = new HashMap<>();
    options.put("userProvider", "ldaps://server.com:636/DC=EXAMPLE,DC=COM");

    assertThrows(
        NullPointerException.class,
        () -> {
          AuthenticationManager.queryUserGroups(
              "CN=Test,DC=example,DC=com", options, "testuser", null);
        });
  }

  @Test
  public void testQueryUserGroupsWithMockLdapSuccess() throws Exception {
    // Mock the static methods
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class)) {
      // Setup test data
      Map<String, String> options = new HashMap<>();
      options.put("userProvider", "ldaps://server.com:636/DC=EXAMPLE,DC=COM");
      options.put("groupNameAttribute", "cn");

      String userDN = "CN=TestUser,OU=Users,DC=example,DC=com";
      String username = "testuser";
      String password = "password";

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapEnvironment(options, username, password))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock search results
      NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
      SearchResult mockSearchResult = mock(SearchResult.class);
      Attributes mockAttributes = mock(Attributes.class);
      Attribute mockGroupAttribute = mock(Attribute.class);

      when(mockResults.hasMore()).thenReturn(true, false);
      when(mockResults.next()).thenReturn(mockSearchResult);
      when(mockSearchResult.getAttributes()).thenReturn(mockAttributes);
      when(mockAttributes.get("cn")).thenReturn(mockGroupAttribute);
      when(mockGroupAttribute.get()).thenReturn("TestGroup");

      when(mockContext.search(anyString(), anyString(), any())).thenReturn(mockResults);

      // Execute the method
      Set<String> result =
          AuthenticationManager.queryUserGroups(userDN, options, username, password);

      // Verify results
      assertNotNull(result);
      assertEquals(1, result.size());
      assertTrue(result.contains("TestGroup"));

      // Verify interactions
      mockedLdapUtil.verify(
          () -> LdapConnectionUtil.createLdapEnvironment(options, username, password));
      mockedLdapUtil.verify(() -> LdapConnectionUtil.createLdapContext(mockEnv));
      mockedLdapUtil.verify(() -> LdapConnectionUtil.logLdapEnvironment(mockEnv));
      verify(mockContext).close();
    }
  }

  @Test
  public void testQueryUserGroupsWithLdapAuthenticationException() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class)) {
      Map<String, String> options = new HashMap<>();
      options.put("userProvider", "ldaps://server.com:636/DC=EXAMPLE,DC=COM");
      options.put("fallbackPrincipal", "CN=Service,DC=example,DC=com");

      String userDN = "CN=TestUser,OU=Users,DC=example,DC=com";
      String username = "testuser";
      String password = "password";

      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapEnvironment(options, username, password))
          .thenReturn(mockEnv);

      // First call throws AuthenticationException
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenThrow(new javax.naming.AuthenticationException("Auth failed"));

      // Execute the method
      Set<String> result =
          AuthenticationManager.queryUserGroups(userDN, options, username, password);

      // Should return empty set when authentication fails
      assertNotNull(result);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testQueryUserGroupsWithCommunicationException() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class)) {
      Map<String, String> options = new HashMap<>();
      options.put("userProvider", "ldaps://server.com:636/DC=EXAMPLE,DC=COM");

      String userDN = "CN=TestUser,OU=Users,DC=example,DC=com";
      String username = "testuser";
      String password = "password";

      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapEnvironment(options, username, password))
          .thenReturn(mockEnv);

      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenThrow(new CommunicationException("Connection failed"));

      Set<String> result =
          AuthenticationManager.queryUserGroups(userDN, options, username, password);

      assertNotNull(result);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testQueryUserGroupsWithNoPermissionException() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class)) {
      Map<String, String> options = new HashMap<>();
      options.put("userProvider", "ldaps://server.com:636/DC=EXAMPLE,DC=COM");

      String userDN = "CN=TestUser,OU=Users,DC=example,DC=com";
      String username = "testuser";
      String password = "password";

      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapEnvironment(options, username, password))
          .thenReturn(mockEnv);

      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenThrow(new NoPermissionException("No permission"));

      Set<String> result =
          AuthenticationManager.queryUserGroups(userDN, options, username, password);

      assertNotNull(result);
      assertTrue(result.isEmpty());
    }
  }

  // ========== Tests for getUserAttributesFromLdap ==========

  @Test
  public void testGetUserAttributesFromLdap() {
    try (MockedStatic<LdapUserAttributeExtractor> mockedExtractor =
        mockStatic(LdapUserAttributeExtractor.class)) {
      String userDN = "CN=TestUser,OU=Users,DC=example,DC=com";
      Map<String, String> ldapOptions = new HashMap<>();
      ldapOptions.put("server", "ldaps://server.com:636");
      String username = "testuser";
      String password = "password";

      Map<String, String> expectedAttributes = new HashMap<>();
      expectedAttributes.put("mail", "test@example.com");
      expectedAttributes.put("displayName", "Test User");

      mockedExtractor
          .when(
              () ->
                  LdapUserAttributeExtractor.fetchLdapUserAttributes(
                      userDN, ldapOptions, username, password))
          .thenReturn(expectedAttributes);

      Map<String, String> result =
          AuthenticationManager.getUserAttributesFromLdap(userDN, ldapOptions, username, password);

      assertEquals(expectedAttributes, result);
      mockedExtractor.verify(
          () ->
              LdapUserAttributeExtractor.fetchLdapUserAttributes(
                  userDN, ldapOptions, username, password));
    }
  }

  // ========== Tests for AuthResult ==========

  @Test
  public void testAuthResultCreation() {
    Set<String> groups = Set.of("group1", "group2");
    String userDN = "CN=Test,DC=example,DC=com";
    Map<String, String> ldapOptions = Map.of("server", "ldaps://server.com");
    Subject subject = new Subject();

    AuthenticationManager.AuthResult authResult =
        new AuthenticationManager.AuthResult(groups, userDN, ldapOptions, subject);

    assertEquals(groups, authResult.groups);
    assertEquals(userDN, authResult.userDN);
    assertEquals(ldapOptions, authResult.ldapOptions);
    assertEquals(subject, authResult.subject);
  }

  @Test
  public void testAuthResultWithNullValues() {
    AuthenticationManager.AuthResult authResult =
        new AuthenticationManager.AuthResult(null, null, null, null);

    assertNull(authResult.groups);
    assertNull(authResult.userDN);
    assertNull(authResult.ldapOptions);
    assertNull(authResult.subject);
  }

  // ========== Helper Classes ==========

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
      return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
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
      return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name);
    }
  }

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
