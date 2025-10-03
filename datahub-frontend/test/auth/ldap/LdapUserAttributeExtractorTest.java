package auth.ldap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import security.AuthenticationManager;

public class LdapUserAttributeExtractorTest {

  private Map<String, String> testOptions;
  private String testUserDN;
  private String testUsername;
  private String testPassword;

  @BeforeEach
  public void setUp() {
    testOptions = new HashMap<>();
    testOptions.put("userProvider", "ldaps://server.example.com:636/DC=example,DC=com");
    testOptions.put("groupProvider", "ldaps://server.example.com:636");

    testUserDN = "CN=TestUser,OU=Users,DC=example,DC=com";
    testUsername = "testuser";
    testPassword = "testpass";
  }

  // ========== Tests for fetchLdapUserAttributes ==========

  @Test
  public void testFetchLdapUserAttributesSuccess() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock base DN extraction
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.extractBaseDN(
                      "ldaps://server.example.com:636/DC=example,DC=com"))
          .thenReturn("DC=example,DC=com");

      // Mock search results
      NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
      SearchResult mockSearchResult = mock(SearchResult.class);
      Attributes mockAttributes = mock(Attributes.class);

      // Mock individual attributes
      Attribute mockMailAttribute = mock(Attribute.class);
      Attribute mockDisplayNameAttribute = mock(Attribute.class);
      Attribute mockGivenNameAttribute = mock(Attribute.class);
      Attribute mockSnAttribute = mock(Attribute.class);
      Attribute mockCnAttribute = mock(Attribute.class);
      Attribute mockUserPrincipalNameAttribute = mock(Attribute.class);

      when(mockResults.hasMore()).thenReturn(true, false);
      when(mockResults.next()).thenReturn(mockSearchResult);
      when(mockSearchResult.getAttributes()).thenReturn(mockAttributes);

      when(mockAttributes.get("mail")).thenReturn(mockMailAttribute);
      when(mockAttributes.get("displayName")).thenReturn(mockDisplayNameAttribute);
      when(mockAttributes.get("givenName")).thenReturn(mockGivenNameAttribute);
      when(mockAttributes.get("sn")).thenReturn(mockSnAttribute);
      when(mockAttributes.get("cn")).thenReturn(mockCnAttribute);
      when(mockAttributes.get("userPrincipalName")).thenReturn(mockUserPrincipalNameAttribute);

      when(mockMailAttribute.get()).thenReturn("test@example.com");
      when(mockDisplayNameAttribute.get()).thenReturn("Test User");
      when(mockGivenNameAttribute.get()).thenReturn("Test");
      when(mockSnAttribute.get()).thenReturn("User");
      when(mockCnAttribute.get()).thenReturn("TestUser");
      when(mockUserPrincipalNameAttribute.get()).thenReturn("testuser@example.com");

      when(mockContext.search(
              eq("DC=example,DC=com"), eq("(objectClass=person)"), any(SearchControls.class)))
          .thenReturn(mockResults);

      // Execute the method
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, testUsername, testPassword);

      // Verify results
      assertNotNull(result);
      assertEquals("test@example.com", result.get("mail"));
      assertEquals("Test User", result.get("displayName"));
      assertEquals("Test", result.get("givenName"));
      assertEquals("User", result.get("sn"));
      assertEquals("TestUser", result.get("cn"));
      assertEquals("testuser@example.com", result.get("userPrincipalName"));

      // Verify interactions
      mockedLdapUtil.verify(
          () -> LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword));
      mockedLdapUtil.verify(() -> LdapConnectionUtil.createLdapContext(mockEnv));
      mockedAuthManager.verify(
          () ->
              AuthenticationManager.extractBaseDN(
                  "ldaps://server.example.com:636/DC=example,DC=com"));
      verify(mockContext)
          .search(eq("DC=example,DC=com"), eq("(objectClass=person)"), any(SearchControls.class));
      verify(mockContext).close();
    }
  }

  @Test
  public void testFetchLdapUserAttributesWithPartialAttributes() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock base DN extraction
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.extractBaseDN(
                      "ldaps://server.example.com:636/DC=example,DC=com"))
          .thenReturn("DC=example,DC=com");

      // Mock search results
      NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
      SearchResult mockSearchResult = mock(SearchResult.class);
      Attributes mockAttributes = mock(Attributes.class);

      // Mock only some attributes (mail and displayName)
      Attribute mockMailAttribute = mock(Attribute.class);
      Attribute mockDisplayNameAttribute = mock(Attribute.class);

      when(mockResults.hasMore()).thenReturn(true, false);
      when(mockResults.next()).thenReturn(mockSearchResult);
      when(mockSearchResult.getAttributes()).thenReturn(mockAttributes);

      when(mockAttributes.get("mail")).thenReturn(mockMailAttribute);
      when(mockAttributes.get("displayName")).thenReturn(mockDisplayNameAttribute);
      when(mockAttributes.get("givenName")).thenReturn(null); // Missing attribute
      when(mockAttributes.get("sn")).thenReturn(null); // Missing attribute
      when(mockAttributes.get("cn")).thenReturn(null); // Missing attribute
      when(mockAttributes.get("userPrincipalName")).thenReturn(null); // Missing attribute

      when(mockMailAttribute.get()).thenReturn("test@example.com");
      when(mockDisplayNameAttribute.get()).thenReturn("Test User");

      when(mockContext.search(
              eq("DC=example,DC=com"), eq("(objectClass=person)"), any(SearchControls.class)))
          .thenReturn(mockResults);

      // Execute the method
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, testUsername, testPassword);

      // Verify results
      assertNotNull(result);
      assertEquals(2, result.size());
      assertEquals("test@example.com", result.get("mail"));
      assertEquals("Test User", result.get("displayName"));
      assertNull(result.get("givenName"));
      assertNull(result.get("sn"));
      assertNull(result.get("cn"));
      assertNull(result.get("userPrincipalName"));

      verify(mockContext).close();
    }
  }

  @Test
  public void testFetchLdapUserAttributesWithNoResults() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock base DN extraction
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.extractBaseDN(
                      "ldaps://server.example.com:636/DC=example,DC=com"))
          .thenReturn("DC=example,DC=com");

      // Mock search results with no results
      NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
      when(mockResults.hasMore()).thenReturn(false);

      when(mockContext.search(
              eq("DC=example,DC=com"), eq("(objectClass=person)"), any(SearchControls.class)))
          .thenReturn(mockResults);

      // Execute the method
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, testUsername, testPassword);

      // Verify results
      assertNotNull(result);
      assertTrue(result.isEmpty());

      verify(mockContext).close();
    }
  }

  @Test
  public void testFetchLdapUserAttributesWithNamingException() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation to throw exception
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenThrow(new NamingException("Connection failed"));

      // Execute the method
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, testUsername, testPassword);

      // Verify results - should return empty map on exception
      assertNotNull(result);
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testFetchLdapUserAttributesWithSearchException() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock base DN extraction
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.extractBaseDN(
                      "ldaps://server.example.com:636/DC=example,DC=com"))
          .thenReturn("DC=example,DC=com");

      // Mock search to throw exception
      when(mockContext.search(
              eq("DC=example,DC=com"), eq("(objectClass=person)"), any(SearchControls.class)))
          .thenThrow(new NamingException("Search failed"));

      // Execute the method
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, testUsername, testPassword);

      // Verify results - should return empty map on exception
      assertNotNull(result);
      assertTrue(result.isEmpty());

      verify(mockContext).close();
    }
  }

  @Test
  public void testFetchLdapUserAttributesWithAttributeException() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock base DN extraction
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.extractBaseDN(
                      "ldaps://server.example.com:636/DC=example,DC=com"))
          .thenReturn("DC=example,DC=com");

      // Mock search results
      NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
      SearchResult mockSearchResult = mock(SearchResult.class);
      Attributes mockAttributes = mock(Attributes.class);

      // Mock attribute that throws exception
      Attribute mockMailAttribute = mock(Attribute.class);

      when(mockResults.hasMore()).thenReturn(true, false);
      when(mockResults.next()).thenReturn(mockSearchResult);
      when(mockSearchResult.getAttributes()).thenReturn(mockAttributes);

      when(mockAttributes.get("mail")).thenReturn(mockMailAttribute);
      when(mockAttributes.get("displayName")).thenReturn(null);
      when(mockAttributes.get("givenName")).thenReturn(null);
      when(mockAttributes.get("sn")).thenReturn(null);
      when(mockAttributes.get("cn")).thenReturn(null);
      when(mockAttributes.get("userPrincipalName")).thenReturn(null);

      when(mockMailAttribute.get()).thenThrow(new NamingException("Attribute access failed"));

      when(mockContext.search(
              eq("DC=example,DC=com"), eq("(objectClass=person)"), any(SearchControls.class)))
          .thenReturn(mockResults);

      // Execute the method
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, testUsername, testPassword);

      // Verify results - should return empty map on exception
      assertNotNull(result);
      assertTrue(result.isEmpty());

      // ctx.close() is now called in the finally block, ensuring proper resource cleanup
      verify(mockContext).close();
    }
  }

  @Test
  public void testFetchLdapUserAttributesWithEmptyBaseDN() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock base DN extraction to return empty string
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.extractBaseDN(
                      "ldaps://server.example.com:636/DC=example,DC=com"))
          .thenReturn("");

      // Mock search results
      NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
      when(mockResults.hasMore()).thenReturn(false);

      when(mockContext.search(eq(""), eq("(objectClass=person)"), any(SearchControls.class)))
          .thenReturn(mockResults);

      // Execute the method
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, testUsername, testPassword);

      // Verify results
      assertNotNull(result);
      assertTrue(result.isEmpty());

      verify(mockContext).close();
    }
  }

  @Test
  public void testFetchLdapUserAttributesWithNullUserDN() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock base DN extraction
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.extractBaseDN(
                      "ldaps://server.example.com:636/DC=example,DC=com"))
          .thenReturn("DC=example,DC=com");

      // Mock search results with no results
      NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
      when(mockResults.hasMore()).thenReturn(false);

      when(mockContext.search(
              eq("DC=example,DC=com"), eq("(objectClass=person)"), any(SearchControls.class)))
          .thenReturn(mockResults);

      // Execute the method with null userDN
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              null, testOptions, testUsername, testPassword);

      // Verify results
      assertNotNull(result);
      assertTrue(result.isEmpty());

      // Verify context was closed
      verify(mockContext).close();
    }
  }

  @Test
  public void testFetchLdapUserAttributesWithNullOptions() {
    // When options is null, the method should handle it gracefully and return empty map
    Map<String, String> result =
        LdapUserAttributeExtractor.fetchLdapUserAttributes(
            testUserDN, null, testUsername, testPassword);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testFetchLdapUserAttributesWithNullUsername() {
    assertThrows(
        NullPointerException.class,
        () -> {
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, null, testPassword);
        });
  }

  @Test
  public void testFetchLdapUserAttributesWithNullPassword() {
    assertThrows(
        NullPointerException.class,
        () -> {
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, testUsername, null);
        });
  }

  @Test
  public void testFetchLdapUserAttributesWithEmptyUserProvider() throws Exception {
    Map<String, String> optionsWithoutUserProvider = new HashMap<>();
    optionsWithoutUserProvider.put("groupProvider", "ldaps://server.example.com:636");

    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(
                      optionsWithoutUserProvider, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock base DN extraction with empty userProvider
      mockedAuthManager.when(() -> AuthenticationManager.extractBaseDN("")).thenReturn("");

      // Mock search results
      NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
      when(mockResults.hasMore()).thenReturn(false);

      when(mockContext.search(eq(""), eq("(objectClass=person)"), any(SearchControls.class)))
          .thenReturn(mockResults);

      // Execute the method
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, optionsWithoutUserProvider, testUsername, testPassword);

      // Verify results
      assertNotNull(result);
      assertTrue(result.isEmpty());

      verify(mockContext).close();
    }
  }

  @Test
  public void testFetchLdapUserAttributesWithMultipleResults() throws Exception {
    try (MockedStatic<LdapConnectionUtil> mockedLdapUtil = mockStatic(LdapConnectionUtil.class);
        MockedStatic<AuthenticationManager> mockedAuthManager =
            mockStatic(AuthenticationManager.class)) {

      // Mock LDAP environment creation
      Hashtable<String, Object> mockEnv = new Hashtable<>();
      mockedLdapUtil
          .when(
              () ->
                  LdapConnectionUtil.createLdapEnvironment(testOptions, testUsername, testPassword))
          .thenReturn(mockEnv);

      // Mock LDAP context creation
      DirContext mockContext = mock(DirContext.class);
      mockedLdapUtil
          .when(() -> LdapConnectionUtil.createLdapContext(mockEnv))
          .thenReturn(mockContext);

      // Mock base DN extraction
      mockedAuthManager
          .when(
              () ->
                  AuthenticationManager.extractBaseDN(
                      "ldaps://server.example.com:636/DC=example,DC=com"))
          .thenReturn("DC=example,DC=com");

      // Mock search results with multiple results (should only process first one)
      NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
      SearchResult mockSearchResult1 = mock(SearchResult.class);
      SearchResult mockSearchResult2 = mock(SearchResult.class);
      Attributes mockAttributes1 = mock(Attributes.class);
      Attributes mockAttributes2 = mock(Attributes.class);

      // Mock first result attributes
      Attribute mockMailAttribute1 = mock(Attribute.class);
      when(mockAttributes1.get("mail")).thenReturn(mockMailAttribute1);
      when(mockAttributes1.get("displayName")).thenReturn(null);
      when(mockAttributes1.get("givenName")).thenReturn(null);
      when(mockAttributes1.get("sn")).thenReturn(null);
      when(mockAttributes1.get("cn")).thenReturn(null);
      when(mockAttributes1.get("userPrincipalName")).thenReturn(null);
      when(mockMailAttribute1.get()).thenReturn("first@example.com");

      // Mock second result attributes (should not be processed)
      Attribute mockMailAttribute2 = mock(Attribute.class);
      when(mockAttributes2.get("mail")).thenReturn(mockMailAttribute2);
      when(mockMailAttribute2.get()).thenReturn("second@example.com");

      when(mockResults.hasMore()).thenReturn(true, true, false);
      when(mockResults.next()).thenReturn(mockSearchResult1, mockSearchResult2);
      when(mockSearchResult1.getAttributes()).thenReturn(mockAttributes1);
      when(mockSearchResult2.getAttributes()).thenReturn(mockAttributes2);

      when(mockContext.search(
              eq("DC=example,DC=com"), eq("(objectClass=person)"), any(SearchControls.class)))
          .thenReturn(mockResults);

      // Execute the method
      Map<String, String> result =
          LdapUserAttributeExtractor.fetchLdapUserAttributes(
              testUserDN, testOptions, testUsername, testPassword);

      // Verify results - should only contain first result
      assertNotNull(result);
      assertEquals(1, result.size());
      assertEquals("first@example.com", result.get("mail"));

      verify(mockContext).close();
    }
  }
}
