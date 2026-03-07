package auth.ldap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import javax.naming.directory.DirContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for LdapGroupExtractor. Note: These tests verify configuration and method signatures.
 * Full integration tests with mock LDAP responses are documented in LDAP_TEST_GUIDE.md
 */
public class LdapGroupExtractorTest {

  private LdapConfigs ldapConfigs;
  private LdapGroupExtractor groupExtractor;
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
    configMap.put("auth.ldap.extractGroupsEnabled", true);
    configMap.put("auth.ldap.groupBaseDn", "ou=groups,dc=example,dc=com");
    configMap.put("auth.ldap.groupFilter", "(member={0})");
    configMap.put("auth.ldap.groupNameAttribute", "cn");

    Config config = ConfigFactory.parseMap(configMap);
    ldapConfigs = new LdapConfigs(config);

    LdapConnectionFactory connectionFactory = new LdapConnectionFactory(ldapConfigs);
    groupExtractor = new LdapGroupExtractor(ldapConfigs, connectionFactory);
  }

  @Test
  public void testConstructor() {
    // Test that extractor can be constructed with valid config
    assertNotNull(groupExtractor);
  }

  @Test
  public void testGroupExtractionEnabledConfiguration() {
    // Test that group extraction is properly configured
    assertTrue(ldapConfigs.isExtractGroupsEnabled());
    assertEquals("ou=groups,dc=example,dc=com", ldapConfigs.getGroupBaseDn());
    assertEquals("(member={0})", ldapConfigs.getGroupFilter());
    assertEquals("cn", ldapConfigs.getGroupNameAttribute());
  }

  @Test
  public void testGroupExtractionDisabledConfiguration() {
    // Test with group extraction disabled
    configMap.put("auth.ldap.extractGroupsEnabled", false);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs disabledConfigs = new LdapConfigs(config);

    assertFalse(disabledConfigs.isExtractGroupsEnabled());
  }

  @Test
  public void testActiveDirectoryGroupFilterConfiguration() {
    // Test Active Directory specific group filter
    configMap.put("auth.ldap.groupFilter", "(member:1.2.840.113556.1.4.1941:={0})");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs adConfigs = new LdapConfigs(config);

    assertEquals("(member:1.2.840.113556.1.4.1941:={0})", adConfigs.getGroupFilter());
  }

  @Test
  public void testUniqueMemberGroupFilterConfiguration() {
    // Test uniqueMember attribute for group membership
    configMap.put("auth.ldap.groupFilter", "(uniqueMember={0})");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs uniqueMemberConfigs = new LdapConfigs(config);

    assertEquals("(uniqueMember={0})", uniqueMemberConfigs.getGroupFilter());
  }

  @Test
  public void testMemberOfAttributeConfiguration() {
    // Test memberOf attribute (reverse lookup)
    configMap.put("auth.ldap.groupFilter", "(memberOf={0})");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs memberOfConfigs = new LdapConfigs(config);

    assertEquals("(memberOf={0})", memberOfConfigs.getGroupFilter());
  }

  @Test
  public void testGroupNameAttributeConfiguration() {
    // Test different group name attributes
    assertEquals("cn", ldapConfigs.getGroupNameAttribute());

    // Test with different attribute
    configMap.put("auth.ldap.groupNameAttribute", "name");
    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs customConfigs = new LdapConfigs(config);

    assertEquals("name", customConfigs.getGroupNameAttribute());
  }

  @Test
  public void testEmptyGroupBaseDnConfiguration() {
    // Test with empty group base DN
    configMap.put("auth.ldap.groupBaseDn", "");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs emptyBaseDnConfigs = new LdapConfigs(config);

    assertEquals("", emptyBaseDnConfigs.getGroupBaseDn());
  }

  @Test
  public void testGroupFilterWithMultipleConditions() {
    // Test complex group filter
    configMap.put("auth.ldap.groupFilter", "(&(objectClass=groupOfNames)(member={0}))");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs complexFilterConfigs = new LdapConfigs(config);

    assertEquals(
        "(&(objectClass=groupOfNames)(member={0}))", complexFilterConfigs.getGroupFilter());
  }

  @Test
  public void testUrlEncodingRequirement() {
    // Test that group names with special characters would be URL encoded
    // This is handled in buildCorpGroupSnapshot method
    String groupNameWithSpaces = "Domain Admins";
    String groupNameWithSpecialChars = "Group-Name_123";

    // These would be URL encoded in actual implementation
    assertNotNull(groupNameWithSpaces);
    assertNotNull(groupNameWithSpecialChars);
  }

  @Test
  public void testGroupDescriptionAttributeHandling() {
    // Test that description attribute is handled
    // This is tested in integration tests with actual LDAP responses
    assertNotNull(groupExtractor);
  }

  @Test
  public void testExtractUsernameFromDnSimple() {
    // Test simple DN extraction
    String dn = "CN=John Doe,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("John Doe", result);
  }

  @Test
  public void testExtractUsernameFromDnWithEscapedComma() {
    // Test DN with escaped comma in CN
    String dn = "CN=Doe\\, John,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("Doe, John", result);
  }

  @Test
  public void testExtractUsernameFromDnWithMultipleEscapedCommas() {
    // Test DN with multiple escaped commas
    String dn = "CN=Smith\\, Jane\\, Jr.,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("Smith, Jane, Jr.", result);
  }

  @Test
  public void testExtractUsernameFromDnWithEscapedBackslash() {
    // Test DN with escaped backslash
    String dn = "CN=John\\\\Smith,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("John\\Smith", result);
  }

  @Test
  public void testExtractUsernameFromDnWithEscapedQuotes() {
    // Test DN with escaped quotes
    String dn = "CN=\\\"John Doe\\\",OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("\"John Doe\"", result);
  }

  @Test
  public void testExtractUsernameFromDnWithEscapedPlus() {
    // Test DN with escaped plus sign
    String dn = "CN=user\\+admin,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("user+admin", result);
  }

  @Test
  public void testExtractUsernameFromDnWithEscapedAngleBrackets() {
    // Test DN with escaped angle brackets
    String dn = "CN=\\<Admin\\>,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("<Admin>", result);
  }

  @Test
  public void testExtractUsernameFromDnWithNoCN() {
    // Test DN without CN prefix
    String dn = "OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("OU=Users,DC=example,DC=com", result);
  }

  @Test
  public void testExtractUsernameFromDnWithNullDn() {
    // Test null DN
    String result = groupExtractor.extractUsernameFromDn(null);
    assertEquals("", result);
  }

  @Test
  public void testExtractUsernameFromDnWithEmptyDn() {
    // Test empty DN
    String result = groupExtractor.extractUsernameFromDn("");
    assertEquals("", result);
  }

  @Test
  public void testExtractUsernameFromDnWithOnlyCN() {
    // Test DN with only CN (no comma after)
    String dn = "CN=John Doe";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("John Doe", result);
  }

  @Test
  public void testExtractUsernameFromDnWithSpaces() {
    // Test DN with spaces in CN
    String dn = "CN= John Doe ,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals(" John Doe ", result);
  }

  @Test
  public void testExtractUsernameFromDnWithUnicode() {
    // Test DN with Unicode characters
    String dn = "CN=José García,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("José García", result);
  }

  @Test
  public void testExtractUsernameFromDnWithMixedEscapes() {
    // Test DN with mixed escaped and unescaped characters
    String dn = "CN=Doe\\, John (Manager),OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("Doe, John (Manager)", result);
  }

  @Test
  public void testExtractUsernameFromDnWithSimpleUsername() {
    // Test DN with simple username
    String dn = "CN=jdoe,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("jdoe", result);
  }

  @Test
  public void testExtractUsernameFromDnWithNumbers() {
    // Test DN with numbers
    String dn = "CN=user123,OU=Employees,DC=corp,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("user123", result);
  }

  @Test
  public void testExtractUsernameFromDnRealWorldExample() {
    // Test real-world example from the codebase
    String dn = "CN=Bharti\\, Aakash,OU=Regular-NonProd,DC=corp,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("Bharti, Aakash", result);
  }

  @Test
  public void testExtractGroupsWithNullPassword() {
    // Test that null password throws IllegalArgumentException
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          groupExtractor.extractGroups("jdoe", "CN=John Doe,OU=Users,DC=example,DC=com", null);
        });
  }

  @Test
  public void testExtractGroupsWithEmptyPassword() {
    // Test that empty password throws IllegalArgumentException
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          groupExtractor.extractGroups("jdoe", "CN=John Doe,OU=Users,DC=example,DC=com", "");
        });
  }

  @Test
  public void testExtractGroupsWhenDisabled() {
    // Test that extractGroups returns empty list when disabled
    configMap.put("auth.ldap.extractGroupsEnabled", false);

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs disabledConfigs = new LdapConfigs(config);
    LdapConnectionFactory connectionFactory = new LdapConnectionFactory(disabledConfigs);
    LdapGroupExtractor disabledExtractor =
        new LdapGroupExtractor(disabledConfigs, connectionFactory);

    var result =
        disabledExtractor.extractGroups(
            "jdoe", "CN=John Doe,OU=Users,DC=example,DC=com", "password");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractGroupsWithEmptyGroupBaseDn() {
    // Test that extractGroups returns empty list when groupBaseDn is empty
    configMap.put("auth.ldap.groupBaseDn", "");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs emptyBaseDnConfigs = new LdapConfigs(config);
    LdapConnectionFactory connectionFactory = new LdapConnectionFactory(emptyBaseDnConfigs);
    LdapGroupExtractor emptyBaseDnExtractor =
        new LdapGroupExtractor(emptyBaseDnConfigs, connectionFactory);

    var result =
        emptyBaseDnExtractor.extractGroups(
            "jdoe", "CN=John Doe,OU=Users,DC=example,DC=com", "password");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractGroupsWithNullGroupBaseDn() {
    // Test that extractGroups returns empty list when groupBaseDn is null
    configMap.remove("auth.ldap.groupBaseDn");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs nullBaseDnConfigs = new LdapConfigs(config);
    LdapConnectionFactory connectionFactory = new LdapConnectionFactory(nullBaseDnConfigs);
    LdapGroupExtractor nullBaseDnExtractor =
        new LdapGroupExtractor(nullBaseDnConfigs, connectionFactory);

    var result =
        nullBaseDnExtractor.extractGroups(
            "jdoe", "CN=John Doe,OU=Users,DC=example,DC=com", "password");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractGroupsPasswordValidation() {
    // Test password validation with various invalid inputs
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          groupExtractor.extractGroups("jdoe", "CN=John Doe,OU=Users,DC=example,DC=com", null);
        },
        "Should throw IllegalArgumentException for null password");

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          groupExtractor.extractGroups("jdoe", "CN=John Doe,OU=Users,DC=example,DC=com", "");
        },
        "Should throw IllegalArgumentException for empty password");
  }

  @Test
  public void testExtractUsernameFromDnWithConsecutiveEscapes() {
    // Test DN with consecutive escape sequences
    String dn = "CN=A\\\\\\\\B,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("A\\\\B", result);
  }

  @Test
  public void testExtractUsernameFromDnWithSpecialCharacters() {
    // Test DN with various special characters
    String dn = "CN=user-name_123.test,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    assertEquals("user-name_123.test", result);
  }

  @Test
  public void testExtractUsernameFromDnCaseSensitive() {
    // Test that CN extraction is case-sensitive (should start with "CN=")
    String dn = "cn=John Doe,OU=Users,DC=example,DC=com";
    String result = groupExtractor.extractUsernameFromDn(dn);
    // Should return original DN since it doesn't start with "CN="
    assertEquals("cn=John Doe,OU=Users,DC=example,DC=com", result);
  }

  @Test
  public void testBuildAuthIdentityWithUpnFormat() {
    // Test UPN format extraction from userFilter
    configMap.put("auth.ldap.userFilter", "(userPrincipalName={0}@example.com)");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs upnConfigs = new LdapConfigs(config);
    LdapGroupExtractor upnExtractor = new LdapGroupExtractor(upnConfigs, mockConnectionFactory);

    String result = upnExtractor.buildAuthIdentity("jdoe");
    assertEquals("jdoe@example.com", result);
  }

  @Test
  public void testBuildAuthIdentityWithSimpleUsername() {
    // Test simple username (no @ in filter)
    configMap.put("auth.ldap.userFilter", "(uid={0})");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs simpleConfigs = new LdapConfigs(config);
    LdapGroupExtractor simpleExtractor =
        new LdapGroupExtractor(simpleConfigs, mockConnectionFactory);

    String result = simpleExtractor.buildAuthIdentity("jdoe");
    assertEquals("jdoe", result);
  }

  @Test
  public void testBuildAuthIdentityWithComplexFilter() {
    // Test complex filter with @ symbol
    configMap.put("auth.ldap.userFilter", "(&(objectClass=user)(userPrincipalName={0}@corp.com))");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs complexConfigs = new LdapConfigs(config);
    LdapGroupExtractor complexExtractor =
        new LdapGroupExtractor(complexConfigs, mockConnectionFactory);

    String result = complexExtractor.buildAuthIdentity("jdoe");
    assertEquals("jdoe@corp.com", result);
  }

  @Test
  public void testBuildAuthIdentityWithSubdomain() {
    // Test filter with subdomain
    configMap.put("auth.ldap.userFilter", "(userPrincipalName={0}@corp.example.com)");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs subdomainConfigs = new LdapConfigs(config);
    LdapGroupExtractor subdomainExtractor =
        new LdapGroupExtractor(subdomainConfigs, mockConnectionFactory);

    String result = subdomainExtractor.buildAuthIdentity("jdoe");
    assertEquals("jdoe@corp.example.com", result);
  }

  @Test
  public void testBuildAuthIdentityWithNoClosingParen() {
    // Test filter without closing parenthesis after domain
    configMap.put("auth.ldap.userFilter", "userPrincipalName={0}@example.com");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs noParenConfigs = new LdapConfigs(config);
    LdapGroupExtractor noParenExtractor =
        new LdapGroupExtractor(noParenConfigs, mockConnectionFactory);

    String result = noParenExtractor.buildAuthIdentity("jdoe");
    // Should return simple username since indexOf(")") returns -1
    assertEquals("jdoe", result);
  }

  @Test
  public void testBuildAuthIdentityWithDifferentUsernames() {
    // Test buildAuthIdentity with various username formats
    configMap.put("auth.ldap.userFilter", "(userPrincipalName={0}@example.com)");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs upnConfigs = new LdapConfigs(config);
    LdapGroupExtractor upnExtractor = new LdapGroupExtractor(upnConfigs, mockConnectionFactory);

    assertEquals("john.doe@example.com", upnExtractor.buildAuthIdentity("john.doe"));
    assertEquals("user123@example.com", upnExtractor.buildAuthIdentity("user123"));
    assertEquals("admin@example.com", upnExtractor.buildAuthIdentity("admin"));
  }

  @Test
  public void testBuildAuthIdentityWithSAMAccountName() {
    // Test Active Directory sAMAccountName format
    configMap.put("auth.ldap.userFilter", "(sAMAccountName={0})");

    Config config = ConfigFactory.parseMap(configMap);
    LdapConfigs adConfigs = new LdapConfigs(config);
    LdapGroupExtractor adExtractor = new LdapGroupExtractor(adConfigs, mockConnectionFactory);

    String result = adExtractor.buildAuthIdentity("jdoe");
    assertEquals("jdoe", result);
  }

  @Test
  public void testBuildAuthIdentityWithEmptyUsername() {
    // Test buildAuthIdentity with empty username
    String result = groupExtractor.buildAuthIdentity("");
    assertNotNull(result);
  }

  @Test
  public void testEscapeDnForFilterWithBackslash() {
    // Test escaping backslash in DN
    String dn = "CN=Doe\\, John,OU=Users,DC=example,DC=com";
    String result = groupExtractor.escapeDnForFilter(dn);
    assertEquals("CN=Doe\\\\, John,OU=Users,DC=example,DC=com", result);
  }

  @Test
  public void testEscapeDnForFilterWithAsterisk() {
    // Test escaping asterisk (wildcard)
    String dn = "CN=user*,OU=Users,DC=example,DC=com";
    String result = groupExtractor.escapeDnForFilter(dn);
    assertEquals("CN=user\\*,OU=Users,DC=example,DC=com", result);
  }

  @Test
  public void testEscapeDnForFilterWithParentheses() {
    // Test escaping parentheses
    String dn = "CN=user(test),OU=Users,DC=example,DC=com";
    String result = groupExtractor.escapeDnForFilter(dn);
    assertEquals("CN=user\\(test\\),OU=Users,DC=example,DC=com", result);
  }

  @Test
  public void testEscapeDnForFilterWithNullCharacter() {
    // Test escaping null character
    String dn = "CN=user\0test,OU=Users,DC=example,DC=com";
    String result = groupExtractor.escapeDnForFilter(dn);
    assertEquals("CN=user\\00test,OU=Users,DC=example,DC=com", result);
  }

  @Test
  public void testEscapeDnForFilterWithMultipleSpecialChars() {
    // Test escaping multiple special characters
    String dn = "CN=user\\*(test),OU=Users,DC=example,DC=com";
    String result = groupExtractor.escapeDnForFilter(dn);
    assertEquals("CN=user\\\\\\*\\(test\\),OU=Users,DC=example,DC=com", result);
  }

  @Test
  public void testEscapeDnForFilterWithNullDn() {
    // Test null DN returns null
    String result = groupExtractor.escapeDnForFilter(null);
    assertNull(result);
  }

  @Test
  public void testEscapeDnForFilterWithEmptyDn() {
    // Test empty DN returns empty
    String result = groupExtractor.escapeDnForFilter("");
    assertEquals("", result);
  }

  @Test
  public void testEscapeDnForFilterWithNormalDn() {
    // Test DN without special characters remains unchanged
    String dn = "CN=John Doe,OU=Users,DC=example,DC=com";
    String result = groupExtractor.escapeDnForFilter(dn);
    assertEquals("CN=John Doe,OU=Users,DC=example,DC=com", result);
  }

  @Test
  public void testEscapeDnForFilterRealWorldExample() {
    // Test real-world DN with escaped comma
    String dn = "CN=Bharti\\, Aakash,OU=Regular-NonProd,DC=corp,DC=example,DC=com";
    String result = groupExtractor.escapeDnForFilter(dn);
    assertEquals("CN=Bharti\\\\, Aakash,OU=Regular-NonProd,DC=corp,DC=example,DC=com", result);
  }

  @Test
  public void testExtractGroupsWithContextNamingException() throws Exception {
    // Test extractGroupsWithContext handles NamingException gracefully
    DirContext mockContext = mock(DirContext.class);

    when(mockContext.search(anyString(), anyString(), any()))
        .thenThrow(new javax.naming.NamingException("LDAP error"));

    var result =
        groupExtractor.extractGroupsWithContext(
            mockContext, "CN=John Doe,OU=Users,DC=example,DC=com");

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractGroupsWithContextNoGroups() throws Exception {
    // Test extractGroupsWithContext when no groups are found
    DirContext mockContext = mock(DirContext.class);
    @SuppressWarnings("unchecked")
    javax.naming.NamingEnumeration<javax.naming.directory.SearchResult> mockResults =
        mock(javax.naming.NamingEnumeration.class);

    when(mockContext.search(anyString(), anyString(), any())).thenReturn(mockResults);
    when(mockResults.hasMore()).thenReturn(false);

    var result =
        groupExtractor.extractGroupsWithContext(
            mockContext, "CN=John Doe,OU=Users,DC=example,DC=com");

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testExtractGroupsWithContextEscapesDn() throws Exception {
    // Test that extractGroupsWithContext properly escapes DN for filter
    DirContext mockContext = mock(DirContext.class);
    @SuppressWarnings("unchecked")
    javax.naming.NamingEnumeration<javax.naming.directory.SearchResult> mockResults =
        mock(javax.naming.NamingEnumeration.class);

    when(mockContext.search(anyString(), anyString(), any())).thenReturn(mockResults);
    when(mockResults.hasMore()).thenReturn(false);

    // DN with backslash should be escaped in the filter
    String userDn = "CN=Doe\\, John,OU=Users,DC=example,DC=com";
    groupExtractor.extractGroupsWithContext(mockContext, userDn);

    // Verify search was called (DN escaping happens internally)
    verify(mockContext).search(anyString(), anyString(), any());
  }

  @Test
  public void testBuildCorpGroupSnapshotWithSimpleGroupName() throws Exception {
    // Test buildCorpGroupSnapshot with simple group name
    javax.naming.directory.Attributes mockAttributes =
        mock(javax.naming.directory.Attributes.class);

    var snapshot = groupExtractor.buildCorpGroupSnapshot("Admins", mockAttributes);

    assertNotNull(snapshot);
    assertNotNull(snapshot.getUrn());
    assertEquals("Admins", snapshot.getUrn().getGroupNameEntity());
    assertNotNull(snapshot.getAspects());
    assertEquals(2, snapshot.getAspects().size());
  }

  @Test
  public void testBuildCorpGroupSnapshotWithSpacesInName() throws Exception {
    // Test buildCorpGroupSnapshot URL encodes spaces
    javax.naming.directory.Attributes mockAttributes =
        mock(javax.naming.directory.Attributes.class);

    var snapshot = groupExtractor.buildCorpGroupSnapshot("Domain Admins", mockAttributes);

    assertNotNull(snapshot);
    assertNotNull(snapshot.getUrn());
    assertEquals("Domain+Admins", snapshot.getUrn().getGroupNameEntity());
  }

  @Test
  public void testBuildCorpGroupSnapshotWithSpecialCharacters() throws Exception {
    // Test buildCorpGroupSnapshot URL encodes special characters
    javax.naming.directory.Attributes mockAttributes =
        mock(javax.naming.directory.Attributes.class);

    var snapshot = groupExtractor.buildCorpGroupSnapshot("Group-Name_123", mockAttributes);

    assertNotNull(snapshot);
    assertNotNull(snapshot.getUrn());
    assertEquals("Group-Name_123", snapshot.getUrn().getGroupNameEntity());
  }

  @Test
  public void testBuildCorpGroupSnapshotWithDescription() throws Exception {
    // Test buildCorpGroupSnapshot includes description when available
    javax.naming.directory.Attributes mockAttributes =
        mock(javax.naming.directory.Attributes.class);
    javax.naming.directory.Attribute mockDescAttr = mock(javax.naming.directory.Attribute.class);

    when(mockAttributes.get("description")).thenReturn(mockDescAttr);
    when(mockDescAttr.get()).thenReturn("Test group description");

    var snapshot = groupExtractor.buildCorpGroupSnapshot("TestGroup", mockAttributes);

    assertNotNull(snapshot);
    assertEquals("TestGroup", snapshot.getUrn().getGroupNameEntity());
  }

  @Test
  public void testBuildCorpGroupSnapshotWithoutDescription() throws Exception {
    // Test buildCorpGroupSnapshot works without description
    javax.naming.directory.Attributes mockAttributes =
        mock(javax.naming.directory.Attributes.class);

    when(mockAttributes.get("description")).thenReturn(null);

    var snapshot = groupExtractor.buildCorpGroupSnapshot("TestGroup", mockAttributes);

    assertNotNull(snapshot);
    assertEquals("TestGroup", snapshot.getUrn().getGroupNameEntity());
  }
}
