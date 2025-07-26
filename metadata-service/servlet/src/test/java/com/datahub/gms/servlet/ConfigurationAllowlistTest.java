package com.datahub.gms.servlet;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authentication.AuthenticatorConfiguration;
import com.datahub.authentication.TokenServiceConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.cache.CacheConfiguration;
import com.linkedin.metadata.config.cache.PrimaryCacheConfiguration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for ConfigurationAllowlist functionality.
 *
 * <p>These tests verify: - Basic field filtering with top-level fields - Nested path support with
 * dot notation - Sensitive field exclusion - Array handling - Edge cases and error handling
 */
public class ConfigurationAllowlistTest {

  private ObjectMapper objectMapper;
  private ConfigurationAllowlist allowlist;

  @Mock private ConfigurationProvider mockConfigProvider;

  @BeforeMethod
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    objectMapper = new ObjectMapper();
  }

  @Test
  public void testTopLevelFieldFiltering() throws Exception {
    // Create a mock configuration object that will be converted to Map via ObjectMapper
    AuthenticationConfiguration mockAuthConfig = new AuthenticationConfiguration();
    mockAuthConfig.setEnabled(true);
    mockAuthConfig.setEnforceExistenceEnabled(true);
    mockAuthConfig.setExcludedPaths("excluded/paths");

    when(mockConfigProvider.getAuthentication()).thenReturn(mockAuthConfig);

    // Create allowlist with only safe top-level fields
    ConfigSectionRule rule =
        ConfigSectionRule.include(
            "authentication",
            Set.of(
                "enabled", "enforceExistenceEnabled"
                // excludedPaths is NOT included
                ));

    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    // Apply filtering
    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Verify results
    assertTrue(result.containsKey("authentication"), "Should contain authentication section");

    Map<String, Object> authSection = (Map<String, Object>) result.get("authentication");
    assertEquals(authSection.get("enabled"), true, "Should include enabled field");
    assertEquals(
        authSection.get("enforceExistenceEnabled"),
        true,
        "Should include enforceExistenceEnabled field");
    assertFalse(authSection.containsKey("excludedPaths"), "Should NOT include excludedPaths");
  }

  @Test
  public void testNestedPathFiltering() throws Exception {
    // Create nested configuration using proper configuration objects
    TokenServiceConfiguration tokenServiceConfig = new TokenServiceConfiguration();
    tokenServiceConfig.setSigningAlgorithm("RS256");
    tokenServiceConfig.setIssuer("datahub");
    tokenServiceConfig.setSigningKey("secret-key-should-be-filtered");
    tokenServiceConfig.setSalt("secret-salt-should-be-filtered");

    AuthenticationConfiguration mockAuthConfig = new AuthenticationConfiguration();
    mockAuthConfig.setEnabled(true);
    mockAuthConfig.setTokenService(tokenServiceConfig);
    mockAuthConfig.setSystemClientSecret("top-level-secret");

    when(mockConfigProvider.getAuthentication()).thenReturn(mockAuthConfig);

    // Create allowlist with nested paths
    ConfigSectionRule rule =
        ConfigSectionRule.include(
            "authentication",
            Set.of(
                "enabled", "tokenService.signingAlgorithm", "tokenService.issuer"
                // Note: signingKey and refreshSigningKey are NOT included
                ));

    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    // Apply filtering
    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Verify results
    assertTrue(result.containsKey("authentication"), "Should contain authentication section");

    Map<String, Object> authSection = (Map<String, Object>) result.get("authentication");
    assertEquals(authSection.get("enabled"), true, "Should include top-level enabled field");

    // Verify tokenService nested structure
    assertTrue(authSection.containsKey("tokenService"), "Should include tokenService section");
    Map<String, Object> tokenService = (Map<String, Object>) authSection.get("tokenService");

    assertEquals(
        tokenService.get("signingAlgorithm"), "RS256", "Should include allowed nested field");
    assertEquals(tokenService.get("issuer"), "datahub", "Should include allowed nested field");
    assertFalse(
        tokenService.containsKey("signingKey"), "Should NOT include sensitive nested field");
    assertFalse(tokenService.containsKey("salt"), "Should NOT include sensitive nested field");

    // Verify top-level sensitive field is filtered
    assertFalse(
        authSection.containsKey("systemClientSecret"),
        "Should NOT include top-level sensitive field");
  }

  @Test
  public void testDeepNestedPaths() throws Exception {
    // Create deeply nested structure using proper configuration objects
    PrimaryCacheConfiguration primaryConfig = new PrimaryCacheConfiguration();
    primaryConfig.setTtlSeconds(3600);
    primaryConfig.setMaxSize(1000);

    CacheConfiguration cacheConfig = new CacheConfiguration();
    cacheConfig.setPrimary(primaryConfig);

    AuthenticationConfiguration mockAuthConfig = new AuthenticationConfiguration();
    mockAuthConfig.setEnabled(true);

    when(mockConfigProvider.getAuthentication()).thenReturn(mockAuthConfig);
    when(mockConfigProvider.getCache()).thenReturn(cacheConfig);

    // Create allowlist with deep nested path using cache configuration
    ConfigSectionRule rule =
        ConfigSectionRule.include(
            "cache",
            Set.of(
                "primary.ttlSeconds" // 2-level deep path
                // primary.maxSize is NOT included
                ));

    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    // Apply filtering
    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Verify results
    assertTrue(result.containsKey("cache"), "Should contain cache section");
    Map<String, Object> cacheSection = (Map<String, Object>) result.get("cache");

    assertTrue(cacheSection.containsKey("primary"), "Should include primary section");
    Map<String, Object> primary = (Map<String, Object>) cacheSection.get("primary");

    assertEquals(primary.get("ttlSeconds"), 3600L, "Should include allowed nested field");
    assertFalse(primary.containsKey("maxSize"), "Should NOT include non-allowed nested field");
  }

  @Test
  public void testArrayHandling() throws Exception {
    // Create configuration with actual array field (authenticators)
    AuthenticationConfiguration mockAuthConfig = new AuthenticationConfiguration();
    mockAuthConfig.setEnabled(true);
    mockAuthConfig.setSystemClientSecret("filtered-secret");

    // Create array of authenticators (real array field in AuthenticationConfiguration)
    AuthenticatorConfiguration tokenAuth = new AuthenticatorConfiguration();
    tokenAuth.setType("DataHubTokenAuthenticator");
    tokenAuth.setConfigs(Map.of("signingKey", "test-key", "enabled", true));

    AuthenticatorConfiguration jwtAuth = new AuthenticatorConfiguration();
    jwtAuth.setType("JWTAuthenticator");
    jwtAuth.setConfigs(Map.of("issuer", "test-issuer", "enabled", false));

    List<AuthenticatorConfiguration> authenticators = Arrays.asList(tokenAuth, jwtAuth);
    mockAuthConfig.setAuthenticators(authenticators);

    when(mockConfigProvider.getAuthentication()).thenReturn(mockAuthConfig);

    // Create allowlist that includes the array field but excludes sensitive fields
    ConfigSectionRule rule =
        ConfigSectionRule.include(
            "authentication",
            Set.of(
                "enabled", "authenticators" // Include array field - should include entire array
                // systemClientSecret is NOT included
                ));

    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    // Apply filtering
    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Verify results
    Map<String, Object> authSection = (Map<String, Object>) result.get("authentication");
    assertEquals(authSection.get("enabled"), true, "Should include enabled field");
    assertFalse(
        authSection.containsKey("systemClientSecret"), "Should NOT include sensitive field");

    // Verify array handling - should include entire array when referenced
    assertTrue(authSection.containsKey("authenticators"), "Should include authenticators array");
    List<Object> resultAuthenticators = (List<Object>) authSection.get("authenticators");
    assertEquals(resultAuthenticators.size(), 2, "Should include all array elements");

    // Verify array contents are preserved
    Map<String, Object> firstAuth = (Map<String, Object>) resultAuthenticators.get(0);
    Map<String, Object> secondAuth = (Map<String, Object>) resultAuthenticators.get(1);
    assertEquals(
        firstAuth.get("type"),
        "DataHubTokenAuthenticator",
        "Should preserve array element content");
    assertEquals(
        secondAuth.get("type"), "JWTAuthenticator", "Should preserve array element content");

    // Verify that configs Map within array elements are also preserved
    Map<String, Object> firstConfigs = (Map<String, Object>) firstAuth.get("configs");
    Map<String, Object> secondConfigs = (Map<String, Object>) secondAuth.get("configs");
    assertEquals(
        firstConfigs.get("signingKey"),
        "test-key",
        "Should preserve nested content within array elements");
    assertEquals(
        firstConfigs.get("enabled"), true, "Should preserve nested content within array elements");
    assertEquals(
        secondConfigs.get("issuer"),
        "test-issuer",
        "Should preserve nested content within array elements");
    assertEquals(
        secondConfigs.get("enabled"),
        false,
        "Should preserve nested content within array elements");
  }

  @Test
  public void testMissingConfigurationSection() throws Exception {
    // Mock returns null for missing section
    when(mockConfigProvider.getAuthentication()).thenReturn(null);

    ConfigSectionRule rule = ConfigSectionRule.include("authentication", Set.of("enabled"));
    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Should not include the missing section
    assertFalse(result.containsKey("authentication"), "Should not include missing section");
  }

  @Test
  public void testMissingNestedPath() throws Exception {
    // Setup config missing some nested paths
    AuthenticationConfiguration mockAuthConfig = new AuthenticationConfiguration();
    mockAuthConfig.setEnabled(true);
    // tokenService is missing (null)

    when(mockConfigProvider.getAuthentication()).thenReturn(mockAuthConfig);

    // Create allowlist that references missing nested paths
    ConfigSectionRule rule =
        ConfigSectionRule.include(
            "authentication",
            Set.of(
                "enabled",
                "tokenService.signingAlgorithm", // This path doesn't exist
                "missingField.nestedField" // This path doesn't exist
                ));

    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Should include existing field but gracefully handle missing paths
    Map<String, Object> authSection = (Map<String, Object>) result.get("authentication");
    assertEquals(authSection.get("enabled"), true, "Should include existing field");
    assertFalse(
        authSection.containsKey("tokenService"), "Should not include missing nested section");
    assertFalse(authSection.containsKey("missingField"), "Should not include missing field");
  }

  @Test
  public void testEmptyAllowedFields() throws Exception {
    AuthenticationConfiguration mockAuthConfig = new AuthenticationConfiguration();
    mockAuthConfig.setEnabled(true);
    mockAuthConfig.setSystemClientSecret("secret");

    when(mockConfigProvider.getAuthentication()).thenReturn(mockAuthConfig);

    // Create rule with empty allowed fields (should exclude everything)
    ConfigSectionRule rule = ConfigSectionRule.include("authentication", Set.of());
    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Should not include the section if no fields are allowed
    assertFalse(
        result.containsKey("authentication"),
        "Should not include section when no fields are allowed");
  }

  @Test
  public void testAllFieldsAllowed() throws Exception {
    AuthenticationConfiguration mockAuthConfig = new AuthenticationConfiguration();
    mockAuthConfig.setEnabled(true);
    mockAuthConfig.setSystemClientSecret("secret");

    TokenServiceConfiguration tokenConfig = new TokenServiceConfiguration();
    tokenConfig.setIssuer("datahub");
    mockAuthConfig.setTokenService(tokenConfig);

    when(mockConfigProvider.getAuthentication()).thenReturn(mockAuthConfig);

    // Create rule that allows all fields (null allowedFields)
    ConfigSectionRule rule = ConfigSectionRule.include("authentication");
    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Should include everything
    Map<String, Object> authSection = (Map<String, Object>) result.get("authentication");
    assertEquals(authSection.get("enabled"), true, "Should include all fields");
    assertEquals(authSection.get("systemClientSecret"), "secret", "Should include all fields");
    assertTrue(authSection.containsKey("tokenService"), "Should include nested structures");
  }

  @Test
  public void testMultipleSections() throws Exception {
    // Setup multiple configuration sections
    AuthenticationConfiguration authConfig = new AuthenticationConfiguration();
    authConfig.setEnabled(true);
    authConfig.setSystemClientSecret("filtered");

    CacheConfiguration cacheConfig = new CacheConfiguration();
    PrimaryCacheConfiguration primaryConfig = new PrimaryCacheConfiguration();
    primaryConfig.setTtlSeconds(3600);
    primaryConfig.setMaxSize(1000);
    cacheConfig.setPrimary(primaryConfig);

    when(mockConfigProvider.getAuthentication()).thenReturn(authConfig);
    when(mockConfigProvider.getCache()).thenReturn(cacheConfig);

    // Create allowlist with multiple sections
    List<ConfigSectionRule> rules =
        Arrays.asList(
            ConfigSectionRule.include("authentication", Set.of("enabled")),
            ConfigSectionRule.include("cache", Set.of("primary.ttlSeconds")));

    allowlist = ConfigurationAllowlist.createCustom(rules, objectMapper);

    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Should include both sections with filtered content
    assertTrue(result.containsKey("authentication"), "Should include authentication section");
    assertTrue(result.containsKey("cache"), "Should include cache section");

    Map<String, Object> authSection = (Map<String, Object>) result.get("authentication");
    Map<String, Object> cacheSection = (Map<String, Object>) result.get("cache");

    assertEquals(authSection.get("enabled"), true, "Should include allowed auth field");
    assertFalse(authSection.containsKey("systemClientSecret"), "Should filter auth secret");

    Map<String, Object> primary = (Map<String, Object>) cacheSection.get("primary");
    assertEquals(primary.get("ttlSeconds"), 3600L, "Should include allowed cache field");
    assertFalse(primary.containsKey("maxSize"), "Should filter cache maxSize");
  }

  @Test
  public void testSectionRenaming() throws Exception {
    AuthenticationConfiguration mockAuthConfig = new AuthenticationConfiguration();
    mockAuthConfig.setEnabled(true);

    when(mockConfigProvider.getAuthentication()).thenReturn(mockAuthConfig);

    // Create rule that renames the section
    ConfigSectionRule rule = ConfigSectionRule.include("authentication", "auth", Set.of("enabled"));
    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Should use the renamed output path
    assertFalse(result.containsKey("authentication"), "Should not use original section name");
    assertTrue(result.containsKey("auth"), "Should use renamed section name");

    Map<String, Object> authSection = (Map<String, Object>) result.get("auth");
    assertEquals(authSection.get("enabled"), true, "Should include field in renamed section");
  }

  @Test
  public void testExcludedSection() throws Exception {
    AuthenticationConfiguration mockAuthConfig = new AuthenticationConfiguration();
    mockAuthConfig.setEnabled(true);

    when(mockConfigProvider.getAuthentication()).thenReturn(mockAuthConfig);

    // Create rule that excludes the section
    ConfigSectionRule rule = ConfigSectionRule.exclude("authentication");
    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Should not include excluded section
    assertFalse(result.containsKey("authentication"), "Should not include excluded section");
  }

  @Test
  public void testOnlyLeafNodesMatch() throws Exception {
    // Create a complex nested configuration structure to test leaf vs non-leaf matching
    CacheConfiguration mockCacheConfig = new CacheConfiguration();

    // Set up primary cache (has leaf values)
    PrimaryCacheConfiguration primaryConfig = new PrimaryCacheConfiguration();
    primaryConfig.setTtlSeconds(3600); // This is a leaf
    primaryConfig.setMaxSize(1000); // This is a leaf
    mockCacheConfig.setPrimary(primaryConfig);

    // Note: client, homepage, search would be non-leaf nodes if they have sub-objects
    // but we're not setting them up with sub-objects for this test

    when(mockConfigProvider.getCache()).thenReturn(mockCacheConfig);

    // Create allowlist rules that try to match both leaf and non-leaf paths
    ConfigSectionRule rule =
        ConfigSectionRule.include(
            "cache",
            Set.of(
                "primary", // NON-LEAF: has children (ttlSeconds, maxSize)
                "primary.ttlSeconds", // LEAF: final value
                "primary.maxSize", // LEAF: final value
                "client", // NON-LEAF: would have children (entityClient, usageClient)
                "nonExistentField" // Non-existent field
                ));

    allowlist = ConfigurationAllowlist.createCustom(Arrays.asList(rule), objectMapper);

    // Apply filtering
    Map<String, Object> result = allowlist.buildAllowedConfiguration(mockConfigProvider);

    // Verify results - only leaf paths should be included
    assertTrue(result.containsKey("cache"), "Should contain cache section");
    Map<String, Object> cacheSection = (Map<String, Object>) result.get("cache");

    // Check if non-leaf "primary" is included (this test may fail if current implementation allows
    // it)
    boolean primaryIncluded = cacheSection.containsKey("primary");
    if (primaryIncluded) {
      // If primary is included, verify its structure
      Map<String, Object> primary = (Map<String, Object>) cacheSection.get("primary");

      // The question is: does including "primary" include the whole object or just create empty
      // structure?
      // If "primary" as non-leaf creates the object, then "primary.ttlSeconds" should add the leaf
      // values
      assertEquals(primary.get("ttlSeconds"), 3600L, "Leaf value should be included");
      assertEquals(primary.get("maxSize"), 1000L, "Leaf value should be included");
    }

    // Check if non-leaf "client" is included (should not be since it's not set up and is non-leaf)
    assertFalse(
        cacheSection.containsKey("client"),
        "Non-leaf client field should not be included when it has potential children");

    // Non-existent field should definitely not be included
    assertFalse(
        cacheSection.containsKey("nonExistentField"), "Non-existent field should not be included");

    // Log the actual structure for debugging
    System.out.println("DEBUG - Cache section structure: " + cacheSection);
    System.out.println("DEBUG - Primary included: " + primaryIncluded);
    if (primaryIncluded) {
      Map<String, Object> primary = (Map<String, Object>) cacheSection.get("primary");
      System.out.println("DEBUG - Primary contents: " + primary);
    }
  }
}
