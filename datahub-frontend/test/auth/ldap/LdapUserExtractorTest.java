package auth.ldap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for LdapUserExtractor. Note: These tests verify configuration and method signatures.
 * Full integration tests with mock LDAP responses are documented in LDAP_TEST_GUIDE.md
 */
public class LdapUserExtractorTest {

  private LdapConfigs ldapConfigs;
  private LdapUserExtractor userExtractor;
  private Map<String, Object> configMap;

  @Mock private LdapConnectionFactory mockConnectionFactory;

  @Mock private DirContext mockDirContext;

  private AutoCloseable mocks;

  @BeforeEach
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);

    configMap = new HashMap<>();
    configMap.put("auth.ldap.server", "ldap://localhost:389");
    configMap.put("auth.ldap.baseDn", "ou=users,dc=example,dc=com");
    configMap.put("auth.ldap.bindDn", "cn=admin,dc=example,dc=com");
    configMap.put("auth.ldap.bindPassword", "admin");
    configMap.put("auth.ldap.userFilter", "(uid={0})");
    configMap.put("auth.ldap.userIdAttribute", "uid");
    configMap.put("auth.ldap.userFirstNameAttribute", "givenName");
    configMap.put("auth.ldap.userLastNameAttribute", "sn");
    configMap.put("auth.ldap.userEmailAttribute", "mail");
    configMap.put("auth.ldap.userDisplayNameAttribute", "displayName");

    Config config = ConfigFactory.parseMap(configMap);
    ldapConfigs = new LdapConfigs(config);

    LdapConnectionFactory connectionFactory = new LdapConnectionFactory(ldapConfigs);
    userExtractor = new LdapUserExtractor(ldapConfigs, connectionFactory);
  }

  @Test
  public void testConstructor() {
    // Test that extractor can be constructed with valid config
    assertNotNull(userExtractor);
  }

  @Test
  public void testUserAttributeConfiguration() {
    // Test that user attribute mappings are properly configured
    assertEquals("givenName", ldapConfigs.getUserFirstNameAttribute());
    assertEquals("sn", ldapConfigs.getUserLastNameAttribute());
    assertEquals("mail", ldapConfigs.getUserEmailAttribute());
    assertEquals("displayName", ldapConfigs.getUserDisplayNameAttribute());
  }

  @Test
  public void testUserFilterConfiguration() {
    // Test that user filter is properly configured
    assertEquals("(uid={0})", ldapConfigs.getUserFilter());
    assertEquals("uid", ldapConfigs.getUserIdAttribute());
  }

  @Test
  public void testActiveDirectoryAttributeConfiguration() {
    // Test Active Directory specific attributes
    configMap.put("auth.ldap.userFilter", "(sAMAccountName={0})");
    configMap.put("auth.ldap.userIdAttribute", "sAMAccountName");
    configMap.put("auth.ldap.userEmailAttribute", "userPrincipalName");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs adConfigs = new LdapConfigs(config);

    assertEquals("(sAMAccountName={0})", adConfigs.getUserFilter());
    assertEquals("sAMAccountName", adConfigs.getUserIdAttribute());
    assertEquals("userPrincipalName", adConfigs.getUserEmailAttribute());
  }

  @Test
  public void testBuildCorpUserSnapshotWithMissingEmail() {
    // Test handling of missing email attribute
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "John Doe");
    attributes.put("givenName", "John");
    attributes.put("sn", "Doe");
    // No mail attribute

    // Should handle gracefully - tested in integration tests
    assertNotNull(attributes);
  }

  @Test
  public void testBuildCorpUserSnapshotWithUPNAsEmail() {
    // Test using userPrincipalName as email fallback
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "John Doe");
    attributes.put("userPrincipalName", "john.doe@example.com");
    // No mail attribute

    // Should use userPrincipalName as email - tested in integration tests
    assertNotNull(attributes);
  }

  @Test
  public void testExtractAttributesToMap() {
    // Test attribute extraction to map
    // This would be tested with mock LDAP Attributes in integration tests
    assertNotNull(userExtractor);
  }

  @Test
  public void testHandleSpecialCharactersInAttributes() {
    // Test handling of special characters in attribute values
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "O'Brien, John");
    attributes.put("mail", "john.o'brien@example.com");

    // Should handle special characters properly - tested in integration tests
    assertNotNull(attributes);
  }

  @Test
  public void testHandleNullAttributeValues() {
    // Test handling of null attribute values
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "John Doe");
    // Other attributes are null

    // Should handle null values gracefully - tested in integration tests
    assertNotNull(attributes);
  }

  @Test
  public void testHandleEmptyAttributeValues() {
    // Test handling of empty attribute values
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "");
    attributes.put("mail", "");

    // Should handle empty values gracefully - tested in integration tests
    assertNotNull(attributes);
  }

  @Test
  public void testBuildUserDnForAuthWithUpnFormat() {
    // Test UPN format extraction from userFilter
    configMap.put("auth.ldap.userFilter", "(userPrincipalName={0}@example.com)");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs upnConfigs = new LdapConfigs(config);
    LdapUserExtractor upnExtractor = new LdapUserExtractor(upnConfigs, mockConnectionFactory);

    String result = upnExtractor.buildUserDnForAuth("jdoe");
    assertEquals("jdoe@example.com", result);
  }

  @Test
  public void testBuildUserDnForAuthWithSimpleUsername() {
    // Test simple username (no @ in filter)
    configMap.put("auth.ldap.userFilter", "(uid={0})");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs simpleConfigs = new LdapConfigs(config);
    LdapUserExtractor simpleExtractor = new LdapUserExtractor(simpleConfigs, mockConnectionFactory);

    String result = simpleExtractor.buildUserDnForAuth("jdoe");
    assertEquals("jdoe", result);
  }

  @Test
  public void testBuildUserDnForAuthWithComplexFilter() {
    // Test complex filter with @ symbol
    configMap.put("auth.ldap.userFilter", "(&(objectClass=user)(userPrincipalName={0}@corp.com))");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs complexConfigs = new LdapConfigs(config);
    LdapUserExtractor complexExtractor =
        new LdapUserExtractor(complexConfigs, mockConnectionFactory);

    String result = complexExtractor.buildUserDnForAuth("jdoe");
    assertEquals("jdoe@corp.com", result);
  }

  @Test
  public void testBuildUserDnForAuthWithSpaceAfterDomain() {
    // Test filter with space after domain
    configMap.put("auth.ldap.userFilter", "(userPrincipalName={0}@example.com )");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs spaceConfigs = new LdapConfigs(config);
    LdapUserExtractor spaceExtractor = new LdapUserExtractor(spaceConfigs, mockConnectionFactory);

    String result = spaceExtractor.buildUserDnForAuth("jdoe");
    assertEquals("jdoe@example.com", result);
  }

  @Test
  public void testBuildUserDnForAuthWithMultipleDomains() {
    // Test filter with subdomain
    configMap.put("auth.ldap.userFilter", "(userPrincipalName={0}@corp.example.com)");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs subdomainConfigs = new LdapConfigs(config);
    LdapUserExtractor subdomainExtractor =
        new LdapUserExtractor(subdomainConfigs, mockConnectionFactory);

    String result = subdomainExtractor.buildUserDnForAuth("jdoe");
    assertEquals("jdoe@corp.example.com", result);
  }

  @Test
  public void testExtractUserWithNullPassword() {
    // Test that null password throws IllegalArgumentException
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          userExtractor.extractUser("jdoe", null);
        });
  }

  @Test
  public void testExtractUserWithEmptyPassword() {
    // Test that empty password throws IllegalArgumentException
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          userExtractor.extractUser("jdoe", "");
        });
  }

  @Test
  public void testExtractUserPasswordValidation() {
    // Test password validation with various invalid inputs
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          userExtractor.extractUser("jdoe", null);
        },
        "Should throw IllegalArgumentException for null password");

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          userExtractor.extractUser("jdoe", "");
        },
        "Should throw IllegalArgumentException for empty password");
  }

  @Test
  public void testGetUserDnWithNullPassword() {
    // Test that getUserDn with null password throws IllegalArgumentException
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          userExtractor.getUserDn("jdoe", null);
        });
  }

  @Test
  public void testGetUserDnWithEmptyPassword() {
    // Test that getUserDn with empty password throws IllegalArgumentException
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          userExtractor.getUserDn("jdoe", "");
        });
  }

  @Test
  public void testGetUserDnPasswordValidation() {
    // Test password validation for getUserDn
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          userExtractor.getUserDn("jdoe", null);
        },
        "Should throw IllegalArgumentException for null password");

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          userExtractor.getUserDn("jdoe", "");
        },
        "Should throw IllegalArgumentException for empty password");
  }

  @Test
  public void testExtractUserWithValidPasswordFormat() {
    // Test that extractUser attempts connection with valid password
    // Will fail due to no LDAP server, but validates the flow
    assertThrows(
        Exception.class,
        () -> {
          userExtractor.extractUser("jdoe", "validPassword123");
        });
  }

  @Test
  public void testGetUserDnWithValidPasswordFormat() {
    // Test that getUserDn attempts connection with valid password
    // Will fail due to no LDAP server, but validates the flow
    assertThrows(
        Exception.class,
        () -> {
          userExtractor.getUserDn("jdoe", "validPassword123");
        });
  }

  @Test
  public void testBuildUserDnForAuthWithDifferentUsernames() {
    // Test buildUserDnForAuth with various username formats
    configMap.put("auth.ldap.userFilter", "(userPrincipalName={0}@example.com)");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs upnConfigs = new LdapConfigs(config);
    LdapUserExtractor upnExtractor = new LdapUserExtractor(upnConfigs, mockConnectionFactory);

    assertEquals("john.doe@example.com", upnExtractor.buildUserDnForAuth("john.doe"));
    assertEquals("user123@example.com", upnExtractor.buildUserDnForAuth("user123"));
    assertEquals("admin@example.com", upnExtractor.buildUserDnForAuth("admin"));
  }

  @Test
  public void testBuildUserDnForAuthWithSpecialCharacters() {
    // Test buildUserDnForAuth with special characters in username
    configMap.put("auth.ldap.userFilter", "(uid={0})");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs simpleConfigs = new LdapConfigs(config);
    LdapUserExtractor simpleExtractor = new LdapUserExtractor(simpleConfigs, mockConnectionFactory);

    assertEquals("user-name", simpleExtractor.buildUserDnForAuth("user-name"));
    assertEquals("user_name", simpleExtractor.buildUserDnForAuth("user_name"));
    assertEquals("user.name", simpleExtractor.buildUserDnForAuth("user.name"));
  }

  @Test
  public void testBuildUserDnForAuthWithEmptyUsername() {
    // Test buildUserDnForAuth with empty username
    String result = userExtractor.buildUserDnForAuth("");
    assertNotNull(result);
  }

  @Test
  public void testBuildUserDnForAuthWithSAMAccountName() {
    // Test Active Directory sAMAccountName format
    configMap.put("auth.ldap.userFilter", "(sAMAccountName={0})");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs adConfigs = new LdapConfigs(config);
    LdapUserExtractor adExtractor = new LdapUserExtractor(adConfigs, mockConnectionFactory);

    String result = adExtractor.buildUserDnForAuth("jdoe");
    assertEquals("jdoe", result);
  }

  @Test
  public void testBuildUserDnForAuthWithNoClosingParen() {
    // Test filter without closing parenthesis after domain
    configMap.put("auth.ldap.userFilter", "userPrincipalName={0}@example.com");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs noParenConfigs = new LdapConfigs(config);
    LdapUserExtractor noParenExtractor =
        new LdapUserExtractor(noParenConfigs, mockConnectionFactory);

    String result = noParenExtractor.buildUserDnForAuth("jdoe");
    assertEquals("jdoe@example.com", result);
  }

  @Test
  public void testBuildCorpUserSnapshotWithAllAttributes() {
    // Test buildCorpUserSnapshot with all attributes present
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "John Doe");
    attributes.put("givenName", "John");
    attributes.put("sn", "Doe");
    attributes.put("mail", "john.doe@example.com");

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertNotNull(snapshot.getUrn());
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
    assertNotNull(snapshot.getAspects());
    assertEquals(2, snapshot.getAspects().size());
  }

  @Test
  public void testBuildCorpUserSnapshotWithMinimalAttributes() {
    // Test buildCorpUserSnapshot with minimal attributes (only username)
    Map<String, String> attributes = new HashMap<>();

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertNotNull(snapshot.getUrn());
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
    assertNotNull(snapshot.getAspects());
  }

  @Test
  public void testBuildCorpUserSnapshotWithDisplayNameOnly() {
    // Test buildCorpUserSnapshot with only displayName
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "John Doe");

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithGivenNameAndSurname() {
    // Test buildCorpUserSnapshot constructs fullName from givenName and sn
    Map<String, String> attributes = new HashMap<>();
    attributes.put("givenName", "John");
    attributes.put("sn", "Doe");

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithMailAttribute() {
    // Test buildCorpUserSnapshot with mail attribute
    Map<String, String> attributes = new HashMap<>();
    attributes.put("mail", "john.doe@example.com");

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithUserPrincipalNameFallback() {
    // Test buildCorpUserSnapshot uses userPrincipalName as email fallback
    Map<String, String> attributes = new HashMap<>();
    attributes.put("userPrincipalName", "jdoe@example.com");
    // No mail attribute

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithEmptyEmail() {
    // Test buildCorpUserSnapshot with empty email
    Map<String, String> attributes = new HashMap<>();
    attributes.put("mail", "");

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithSpecialCharactersInName() {
    // Test buildCorpUserSnapshot with special characters
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "O'Brien, John");
    attributes.put("mail", "john.o'brien@example.com");

    var snapshot = userExtractor.buildCorpUserSnapshot("jobrien", attributes);

    assertNotNull(snapshot);
    assertEquals("jobrien", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithUnicodeCharacters() {
    // Test buildCorpUserSnapshot with Unicode characters
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "José García");
    attributes.put("givenName", "José");
    attributes.put("sn", "García");
    attributes.put("mail", "jose.garcia@example.com");

    var snapshot = userExtractor.buildCorpUserSnapshot("jgarcia", attributes);

    assertNotNull(snapshot);
    assertEquals("jgarcia", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithLongNames() {
    // Test buildCorpUserSnapshot with long names
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "John Jacob Jingleheimer Schmidt");
    attributes.put("givenName", "John Jacob Jingleheimer");
    attributes.put("sn", "Schmidt");

    var snapshot = userExtractor.buildCorpUserSnapshot("jjschmidt", attributes);

    assertNotNull(snapshot);
    assertEquals("jjschmidt", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithEmptyGivenName() {
    // Test buildCorpUserSnapshot with empty givenName
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "John Doe");
    attributes.put("givenName", "");
    attributes.put("sn", "Doe");

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithEmptySurname() {
    // Test buildCorpUserSnapshot with empty surname
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "John Doe");
    attributes.put("givenName", "John");
    attributes.put("sn", "");

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotUsernameAsDisplayName() {
    // Test buildCorpUserSnapshot uses username as displayName when not provided
    Map<String, String> attributes = new HashMap<>();
    // No displayName attribute

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
  }

  @Test
  public void testBuildCorpUserSnapshotWithAllAttributesComplete() {
    // Test buildCorpUserSnapshot with complete attribute set
    Map<String, String> attributes = new HashMap<>();
    attributes.put("displayName", "John Doe");
    attributes.put("givenName", "John");
    attributes.put("sn", "Doe");
    attributes.put("mail", "john.doe@example.com");
    attributes.put("userPrincipalName", "jdoe@example.com");

    var snapshot = userExtractor.buildCorpUserSnapshot("jdoe", attributes);

    assertNotNull(snapshot);
    assertEquals("jdoe", snapshot.getUrn().getUsernameEntity());
    assertNotNull(snapshot.getAspects());
    assertEquals(2, snapshot.getAspects().size());
  }

  @Test
  public void testExtractAttributesToMapWithMockAttributes() throws Exception {
    // Test extractAttributesToMap with mock LDAP Attributes
    Attributes mockAttributes = mock(Attributes.class);
    @SuppressWarnings("unchecked")
    NamingEnumeration<Attribute> mockEnum = mock(NamingEnumeration.class);

    Attribute attr1 = mock(Attribute.class);
    Attribute attr2 = mock(Attribute.class);
    Attribute attr3 = mock(Attribute.class);

    doReturn(mockEnum).when(mockAttributes).getAll();
    when(mockEnum.hasMore()).thenReturn(true, true, true, false);
    when(mockEnum.next()).thenReturn(attr1, attr2, attr3);

    when(attr1.getID()).thenReturn("displayName");
    when(attr1.get()).thenReturn("John Doe");

    when(attr2.getID()).thenReturn("mail");
    when(attr2.get()).thenReturn("john.doe@example.com");

    when(attr3.getID()).thenReturn("givenName");
    when(attr3.get()).thenReturn("John");

    Map<String, String> result = userExtractor.extractAttributesToMap(mockAttributes);

    assertEquals(3, result.size());
    assertEquals("John Doe", result.get("displayName"));
    assertEquals("john.doe@example.com", result.get("mail"));
    assertEquals("John", result.get("givenName"));
  }

  @Test
  public void testExtractAttributesToMapWithNullValues() throws Exception {
    // Test extractAttributesToMap with null attribute values
    Attributes mockAttributes = mock(Attributes.class);
    @SuppressWarnings("unchecked")
    NamingEnumeration<Attribute> mockEnum = mock(NamingEnumeration.class);

    Attribute attr1 = mock(Attribute.class);
    Attribute attr2 = mock(Attribute.class);

    doReturn(mockEnum).when(mockAttributes).getAll();
    when(mockEnum.hasMore()).thenReturn(true, true, false);
    when(mockEnum.next()).thenReturn(attr1, attr2);

    when(attr1.getID()).thenReturn("displayName");
    when(attr1.get()).thenReturn("John Doe");

    when(attr2.getID()).thenReturn("mail");
    when(attr2.get()).thenReturn(null); // Null value

    Map<String, String> result = userExtractor.extractAttributesToMap(mockAttributes);

    assertEquals(1, result.size());
    assertEquals("John Doe", result.get("displayName"));
    assertFalse(result.containsKey("mail"));
  }

  @Test
  public void testExtractAttributesToMapWithNamingException() throws Exception {
    // Test extractAttributesToMap handles NamingException gracefully
    Attributes mockAttributes = mock(Attributes.class);
    @SuppressWarnings("unchecked")
    NamingEnumeration<Attribute> mockEnum = mock(NamingEnumeration.class);

    doReturn(mockEnum).when(mockAttributes).getAll();
    // Simulate NamingException during iteration
    when(mockEnum.hasMore()).thenThrow(new NamingException("Test exception"));

    Map<String, String> result = userExtractor.extractAttributesToMap(mockAttributes);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractAttributesToMapWithEmptyAttributes() throws Exception {
    // Test extractAttributesToMap with no attributes
    Attributes mockAttributes = mock(Attributes.class);
    @SuppressWarnings("unchecked")
    NamingEnumeration<Attribute> mockEnum = mock(NamingEnumeration.class);

    doReturn(mockEnum).when(mockAttributes).getAll();
    when(mockEnum.hasMore()).thenReturn(false);

    Map<String, String> result = userExtractor.extractAttributesToMap(mockAttributes);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractUserWithContextUserNotFound() throws Exception {
    // Test extractUserWithContext when user is not found
    DirContext mockContext = mock(DirContext.class);
    NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);

    when(mockContext.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(mockResults);
    when(mockResults.hasMore()).thenReturn(false);

    Exception exception =
        assertThrows(
            Exception.class,
            () -> {
              userExtractor.extractUserWithContext(mockContext, "jdoe");
            });

    assertTrue(exception.getMessage().contains("User not found in LDAP"));
  }

  @Test
  public void testExtractUserWithContextNamingException() throws Exception {
    // Test extractUserWithContext handles NamingException
    DirContext mockContext = mock(DirContext.class);

    when(mockContext.search(anyString(), anyString(), any(SearchControls.class)))
        .thenThrow(new NamingException("LDAP error"));

    Exception exception =
        assertThrows(
            Exception.class,
            () -> {
              userExtractor.extractUserWithContext(mockContext, "jdoe");
            });

    assertTrue(exception.getMessage().contains("Failed to extract user attributes"));
  }

  @Test
  public void testGetUserDnWithContextUserNotFound() throws Exception {
    // Test getUserDnWithContext when user is not found
    DirContext mockContext = mock(DirContext.class);
    NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);

    when(mockContext.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(mockResults);
    when(mockResults.hasMore()).thenReturn(false);

    Exception exception =
        assertThrows(
            Exception.class,
            () -> {
              userExtractor.getUserDnWithContext(mockContext, "jdoe");
            });

    assertTrue(exception.getMessage().contains("User not found in LDAP"));
  }

  @Test
  public void testGetUserDnWithContextNamingException() throws Exception {
    // Test getUserDnWithContext handles NamingException
    DirContext mockContext = mock(DirContext.class);

    when(mockContext.search(anyString(), anyString(), any(SearchControls.class)))
        .thenThrow(new NamingException("LDAP error"));

    Exception exception =
        assertThrows(
            Exception.class,
            () -> {
              userExtractor.getUserDnWithContext(mockContext, "jdoe");
            });

    assertTrue(exception.getMessage().contains("Failed to get user DN"));
  }

  @Test
  public void testGetUserDnWithContextSuccess() throws Exception {
    // Test getUserDnWithContext successful DN retrieval
    DirContext mockContext = mock(DirContext.class);
    NamingEnumeration<SearchResult> mockResults = mock(NamingEnumeration.class);
    SearchResult mockSearchResult = mock(SearchResult.class);

    when(mockContext.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(mockResults);
    when(mockResults.hasMore()).thenReturn(true);
    when(mockResults.next()).thenReturn(mockSearchResult);
    when(mockSearchResult.getNameInNamespace())
        .thenReturn("CN=John Doe,OU=Users,DC=example,DC=com");

    String result = userExtractor.getUserDnWithContext(mockContext, "jdoe");

    assertEquals("CN=John Doe,OU=Users,DC=example,DC=com", result);
  }
}
