package datahub.client.v2.integration;

import static org.junit.Assume.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import datahub.client.v2.DataHubClientV2;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Base class for integration tests that provides authenticated DataHub client.
 *
 * <p>This class handles authentication with DataHub server and manages access token lifecycle:
 *
 * <ul>
 *   <li>Authenticates with admin credentials (ADMIN_USERNAME/ADMIN_PASSWORD or defaults)
 *   <li>Generates a personal access token for API calls
 *   <li>Provides authenticated DataHubClientV2 instance
 *   <li>Automatically revokes token after all tests complete
 * </ul>
 *
 * <p>Required environment variables:
 *
 * <ul>
 *   <li>DATAHUB_SERVER: DataHub GMS server URL (default: http://localhost:8080)
 * </ul>
 *
 * <p>Optional environment variables:
 *
 * <ul>
 *   <li>ADMIN_USERNAME: Admin username (default: admin)
 *   <li>ADMIN_PASSWORD: Admin password (default: mypass)
 * </ul>
 *
 * <p>To run integration tests: ./gradlew :metadata-integration:java:datahub-client:test --tests
 * "*Integration*"
 */
public abstract class BaseIntegrationTest {

  protected static DataHubClientV2 client;

  private static final String DEFAULT_SERVER = "http://localhost:8080";
  private static final String DEFAULT_USERNAME = "datahub";
  private static final String DEFAULT_PASSWORD = "datahub";
  private static final String DEFAULT_FRONTEND_PORT = "9002";

  // Only use defaults for local dev - CI should explicitly set DATAHUB_SERVER or tests skip
  protected static final String TEST_SERVER =
      System.getenv("CI") != null
          ? System.getenv("DATAHUB_SERVER")
          : getEnvOrDefault("DATAHUB_SERVER", DEFAULT_SERVER);
  private static final String ADMIN_USERNAME = getEnvOrDefault("ADMIN_USERNAME", DEFAULT_USERNAME);
  private static final String ADMIN_PASSWORD = getEnvOrDefault("ADMIN_PASSWORD", DEFAULT_PASSWORD);

  private static String generatedToken;
  private static String generatedTokenId;
  private static final HttpClient httpClient = HttpClient.newHttpClient();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeClass
  public static void setupClass() throws Exception {
    // Skip if server not available
    assumeTrue(
        "DATAHUB_SERVER not set or empty, skipping integration tests",
        TEST_SERVER != null && !TEST_SERVER.isEmpty());

    // Check if token was provided via environment variable
    String providedToken = System.getenv("DATAHUB_TOKEN");
    if (providedToken != null && !providedToken.isEmpty()) {
      System.out.println("Using DATAHUB_TOKEN from environment");
      generatedToken = providedToken;
      generatedTokenId = null; // Don't revoke externally provided tokens
    } else {
      System.out.println("No DATAHUB_TOKEN provided, generating new token...");
      authenticateAndGenerateToken();
    }

    // Build client with token (SDK mode defaults to async=false for synchronous DB writes)
    client = DataHubClientV2.builder().server(TEST_SERVER).token(generatedToken).build();

    // Test connection
    try {
      boolean connected = client.testConnection();
      assumeTrue("Cannot connect to DataHub server at " + TEST_SERVER, connected);
      System.out.println("✓ Successfully connected to DataHub at " + TEST_SERVER);
    } catch (Exception e) {
      assumeTrue("Cannot connect to DataHub server: " + e.getMessage(), false);
    }
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    if (client != null) {
      client.close();
    }

    // Revoke token if we generated it (don't revoke externally provided tokens)
    if (generatedTokenId != null) {
      try {
        revokeAccessToken();
        System.out.println("✓ Access token revoked");
      } catch (Exception e) {
        System.err.println("Warning: Failed to revoke access token: " + e.getMessage());
      }
    }
  }

  @Before
  public void setup() {
    // Individual test setup - can be overridden by subclasses
  }

  @After
  public void teardown() {
    // Individual test teardown - can be overridden by subclasses
  }

  private static void authenticateAndGenerateToken() throws Exception {
    // Step 1: Login to get session cookie
    String sessionCookie = login();
    System.out.println("✓ Authenticated as " + ADMIN_USERNAME);

    // Step 2: Generate personal access token
    generateAccessToken(sessionCookie);
    System.out.println("✓ Generated personal access token (ID: " + generatedTokenId + ")");
  }

  private static String login() throws Exception {
    String frontendUrl = getFrontendUrl();
    URI loginUri = URI.create(frontendUrl + "/logIn");

    Map<String, String> loginPayload = new HashMap<>();
    loginPayload.put("username", ADMIN_USERNAME);
    loginPayload.put("password", ADMIN_PASSWORD);

    String requestBody = objectMapper.writeValueAsString(loginPayload);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(loginUri)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Login failed with status " + response.statusCode() + ": " + response.body());
    }

    // Extract ALL session cookies (both PLAY_SESSION and actor)
    List<String> cookies = response.headers().allValues("set-cookie");
    StringBuilder cookieHeader = new StringBuilder();
    for (String cookie : cookies) {
      String cookieValue = cookie.split(";")[0]; // Get just "name=value" part
      if (cookieHeader.length() > 0) {
        cookieHeader.append("; ");
      }
      cookieHeader.append(cookieValue);
    }

    if (cookieHeader.length() == 0) {
      throw new RuntimeException("No session cookies found in login response");
    }

    return cookieHeader.toString();
  }

  private static void generateAccessToken(String sessionCookie) throws Exception {
    String frontendUrl = getFrontendUrl();
    URI graphqlUri = URI.create(frontendUrl + "/api/v2/graphql");

    // Extract actor URN from cookie
    String actorUrn = "urn:li:corpuser:" + ADMIN_USERNAME;

    Map<String, Object> graphqlPayload = new HashMap<>();
    graphqlPayload.put(
        "query",
        "mutation createAccessToken($input: CreateAccessTokenInput!) {"
            + "  createAccessToken(input: $input) {"
            + "    accessToken"
            + "    metadata { id }"
            + "  }"
            + "}");

    Map<String, Object> input = new HashMap<>();
    input.put("type", "PERSONAL");
    input.put("actorUrn", actorUrn);
    input.put("duration", "ONE_DAY");
    input.put("name", "Java SDK V2 Integration Tests");
    input.put("description", "Token for Java SDK V2 integration tests");

    Map<String, Object> variables = new HashMap<>();
    variables.put("input", input);
    graphqlPayload.put("variables", variables);

    String requestBody = objectMapper.writeValueAsString(graphqlPayload);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(graphqlUri)
            .header("Content-Type", "application/json")
            .header("Cookie", sessionCookie)
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Token generation failed with status " + response.statusCode() + ": " + response.body());
    }

    // Parse response
    JsonNode root = objectMapper.readTree(response.body());
    JsonNode data = root.path("data").path("createAccessToken");

    if (data.isMissingNode()) {
      throw new RuntimeException("Token generation failed: " + root.path("errors").toString());
    }

    generatedToken = data.path("accessToken").asText();
    generatedTokenId = data.path("metadata").path("id").asText();
  }

  private static void revokeAccessToken() throws Exception {
    String frontendUrl = getFrontendUrl();
    URI graphqlUri = URI.create(frontendUrl + "/api/v2/graphql");

    Map<String, Object> graphqlPayload = new HashMap<>();
    graphqlPayload.put(
        "query",
        "mutation revokeAccessToken($tokenId: String!) {"
            + "  revokeAccessToken(tokenId: $tokenId)"
            + "}");

    Map<String, String> variables = new HashMap<>();
    variables.put("tokenId", generatedTokenId);
    graphqlPayload.put("variables", variables);

    String requestBody = objectMapper.writeValueAsString(graphqlPayload);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(graphqlUri)
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer " + generatedToken)
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Token revocation failed with status " + response.statusCode() + ": " + response.body());
    }
  }

  private static String getFrontendUrl() {
    // Frontend typically runs on port 9002, GMS on 8080
    // Convert GMS URL to frontend URL
    String serverUrl = TEST_SERVER;
    if (serverUrl.contains(":8080")) {
      return serverUrl.replace(":8080", ":" + DEFAULT_FRONTEND_PORT);
    }
    // If custom port, assume frontend is on GMS_PORT + 1002
    return serverUrl; // Fallback to same URL
  }

  private static String getEnvOrDefault(String key, String defaultValue) {
    String value = System.getenv(key);
    return (value != null && !value.isEmpty()) ? value : defaultValue;
  }

  /**
   * Validates that an entity has specific tags using SDK client.entities().get().
   *
   * @param urn the URN of the entity
   * @param entityClass the entity class
   * @param expectedTags list of tag names that should be present
   * @param <T> the entity type
   * @throws Exception if validation fails
   */
  protected static <T extends datahub.client.v2.entity.Entity> void validateEntityHasTags(
      String urn, Class<T> entityClass, List<String> expectedTags) throws Exception {
    T entity = client.entities().get(urn, entityClass);
    if (entity == null) {
      throw new AssertionError(String.format("Entity %s not found", urn));
    }

    if (!(entity instanceof datahub.client.v2.entity.HasTags)) {
      throw new AssertionError(
          String.format("Entity %s does not implement HasTags interface", urn));
    }

    List<com.linkedin.common.TagAssociation> tags =
        ((datahub.client.v2.entity.HasTags<?>) entity).getTags();

    if (tags == null || tags.isEmpty()) {
      throw new AssertionError(
          String.format("Entity %s does not have tags or tags are empty", urn));
    }

    List<String> actualTags = new ArrayList<>();
    for (com.linkedin.common.TagAssociation tagAssoc : tags) {
      String tagUrn = tagAssoc.getTag().toString();
      String tagName = tagUrn.substring(tagUrn.lastIndexOf(':') + 1);
      actualTags.add(tagName);
    }

    for (String expectedTag : expectedTags) {
      if (!actualTags.contains(expectedTag)) {
        throw new AssertionError(
            String.format(
                "Entity %s missing expected tag '%s'. Actual tags: %s",
                urn, expectedTag, actualTags));
      }
    }
  }

  /**
   * Validates that an entity has specific owners using SDK client.entities().get().
   *
   * @param urn the URN of the entity
   * @param entityClass the entity class
   * @param expectedOwnerUrns list of owner URNs that should be present
   * @param <T> the entity type
   * @throws Exception if validation fails
   */
  protected static <T extends datahub.client.v2.entity.Entity> void validateEntityHasOwners(
      String urn, Class<T> entityClass, List<String> expectedOwnerUrns) throws Exception {
    T entity = client.entities().get(urn, entityClass);
    if (entity == null) {
      throw new AssertionError(String.format("Entity %s not found", urn));
    }

    if (!(entity instanceof datahub.client.v2.entity.HasOwners)) {
      throw new AssertionError(
          String.format("Entity %s does not implement HasOwners interface", urn));
    }

    List<com.linkedin.common.Owner> owners =
        ((datahub.client.v2.entity.HasOwners<?>) entity).getOwners();

    if (owners == null || owners.isEmpty()) {
      throw new AssertionError(
          String.format("Entity %s does not have owners or owners are empty", urn));
    }

    List<String> actualOwnerUrns = new ArrayList<>();
    for (com.linkedin.common.Owner owner : owners) {
      actualOwnerUrns.add(owner.getOwner().toString());
    }

    for (String expectedOwnerUrn : expectedOwnerUrns) {
      if (!actualOwnerUrns.contains(expectedOwnerUrn)) {
        throw new AssertionError(
            String.format(
                "Entity %s missing expected owner '%s'. Actual owners: %s",
                urn, expectedOwnerUrn, actualOwnerUrns));
      }
    }
  }

  /**
   * Validates that an entity has a specific description using SDK client.entities().get().
   *
   * @param urn the URN of the entity
   * @param entityClass the entity class
   * @param expectedDescription the expected description text
   * @param <T> the entity type
   * @throws Exception if validation fails
   */
  protected static <T extends datahub.client.v2.entity.Entity> void validateEntityDescription(
      String urn, Class<T> entityClass, String expectedDescription) throws Exception {
    T entity = client.entities().get(urn, entityClass);
    if (entity == null) {
      throw new AssertionError(String.format("Entity %s not found", urn));
    }

    String actualDescription = null;
    if (entity instanceof datahub.client.v2.entity.Dataset) {
      actualDescription = ((datahub.client.v2.entity.Dataset) entity).getDescription();
    } else if (entity instanceof datahub.client.v2.entity.Chart) {
      actualDescription = ((datahub.client.v2.entity.Chart) entity).getDescription();
    } else if (entity instanceof datahub.client.v2.entity.Dashboard) {
      actualDescription = ((datahub.client.v2.entity.Dashboard) entity).getDescription();
    } else if (entity instanceof datahub.client.v2.entity.DataJob) {
      actualDescription = ((datahub.client.v2.entity.DataJob) entity).getDescription();
    } else if (entity instanceof datahub.client.v2.entity.MLModel) {
      actualDescription = ((datahub.client.v2.entity.MLModel) entity).getDescription();
    } else if (entity instanceof datahub.client.v2.entity.DataFlow) {
      actualDescription = ((datahub.client.v2.entity.DataFlow) entity).getDescription();
    } else if (entity instanceof datahub.client.v2.entity.Container) {
      actualDescription = ((datahub.client.v2.entity.Container) entity).getDescription();
    } else if (entity instanceof datahub.client.v2.entity.MLModelGroup) {
      actualDescription = ((datahub.client.v2.entity.MLModelGroup) entity).getDescription();
    }

    if (actualDescription == null) {
      throw new AssertionError(
          String.format("Entity %s does not have a description property", urn));
    }

    if (!expectedDescription.equals(actualDescription)) {
      throw new AssertionError(
          String.format(
              "Entity %s description mismatch. Expected: '%s', Actual: '%s'",
              urn, expectedDescription, actualDescription));
    }
  }

  /**
   * Validates that an entity has specific custom properties using SDK client.entities().get().
   *
   * @param urn the URN of the entity
   * @param entityClass the entity class
   * @param expectedCustomProperties map of custom property keys to expected values
   * @param <T> the entity type
   * @throws Exception if validation fails
   */
  protected static <T extends datahub.client.v2.entity.Entity> void validateEntityCustomProperties(
      String urn, Class<T> entityClass, Map<String, String> expectedCustomProperties)
      throws Exception {
    T entity = client.entities().get(urn, entityClass);
    if (entity == null) {
      throw new AssertionError(String.format("Entity %s not found", urn));
    }

    Map<String, String> actualCustomProps = new HashMap<>();

    if (entity instanceof datahub.client.v2.entity.Dataset) {
      com.linkedin.dataset.DatasetProperties props =
          entity.getAspectLazy(com.linkedin.dataset.DatasetProperties.class);
      if (props != null && props.hasCustomProperties()) {
        actualCustomProps.putAll(props.getCustomProperties());
      }
    } else if (entity instanceof datahub.client.v2.entity.Chart) {
      com.linkedin.chart.ChartInfo info = entity.getAspectLazy(com.linkedin.chart.ChartInfo.class);
      if (info != null && info.hasCustomProperties()) {
        actualCustomProps.putAll(info.getCustomProperties());
      }
    } else if (entity instanceof datahub.client.v2.entity.Dashboard) {
      com.linkedin.dashboard.DashboardInfo info =
          entity.getAspectLazy(com.linkedin.dashboard.DashboardInfo.class);
      if (info != null && info.hasCustomProperties()) {
        actualCustomProps.putAll(info.getCustomProperties());
      }
    } else if (entity instanceof datahub.client.v2.entity.DataJob) {
      com.linkedin.datajob.DataJobInfo info =
          entity.getAspectLazy(com.linkedin.datajob.DataJobInfo.class);
      if (info != null && info.hasCustomProperties()) {
        actualCustomProps.putAll(info.getCustomProperties());
      }
    } else if (entity instanceof datahub.client.v2.entity.MLModel) {
      com.linkedin.ml.metadata.MLModelProperties props =
          entity.getAspectLazy(com.linkedin.ml.metadata.MLModelProperties.class);
      if (props != null && props.hasCustomProperties()) {
        actualCustomProps.putAll(props.getCustomProperties());
      }
    } else if (entity instanceof datahub.client.v2.entity.DataFlow) {
      com.linkedin.datajob.DataFlowInfo info =
          entity.getAspectLazy(com.linkedin.datajob.DataFlowInfo.class);
      if (info != null && info.hasCustomProperties()) {
        actualCustomProps.putAll(info.getCustomProperties());
      }
    } else if (entity instanceof datahub.client.v2.entity.Container) {
      com.linkedin.container.ContainerProperties props =
          entity.getAspectLazy(com.linkedin.container.ContainerProperties.class);
      if (props != null && props.hasCustomProperties()) {
        actualCustomProps.putAll(props.getCustomProperties());
      }
    }

    if (actualCustomProps.isEmpty() && !expectedCustomProperties.isEmpty()) {
      throw new AssertionError(String.format("Entity %s does not have custom properties", urn));
    }

    for (Map.Entry<String, String> entry : expectedCustomProperties.entrySet()) {
      String key = entry.getKey();
      String expectedValue = entry.getValue();

      if (!actualCustomProps.containsKey(key)) {
        throw new AssertionError(
            String.format(
                "Entity %s missing custom property '%s'. Actual properties: %s",
                urn, key, actualCustomProps.keySet()));
      }

      String actualValue = actualCustomProps.get(key);
      if (!expectedValue.equals(actualValue)) {
        throw new AssertionError(
            String.format(
                "Entity %s custom property '%s' mismatch. Expected: '%s', Actual: '%s'",
                urn, key, expectedValue, actualValue));
      }
    }
  }

  /**
   * Validates that an entity has specific subtypes using SDK client.entities().get().
   *
   * @param urn the URN of the entity
   * @param entityClass the entity class
   * @param expectedSubTypes list of subtype names that should be present
   * @param <T> the entity type
   * @throws Exception if validation fails
   */
  protected static <T extends datahub.client.v2.entity.Entity> void validateEntityHasSubTypes(
      String urn, Class<T> entityClass, List<String> expectedSubTypes) throws Exception {
    T entity = client.entities().get(urn, entityClass);
    if (entity == null) {
      throw new AssertionError(String.format("Entity %s not found", urn));
    }

    com.linkedin.common.SubTypes subTypes =
        entity.getAspectLazy(com.linkedin.common.SubTypes.class);

    if (subTypes == null || !subTypes.hasTypeNames()) {
      throw new AssertionError(
          String.format("Entity %s does not have subTypes aspect or subTypes are empty", urn));
    }

    List<String> actualSubTypes = new ArrayList<>(subTypes.getTypeNames());

    for (String expectedSubType : expectedSubTypes) {
      if (!actualSubTypes.contains(expectedSubType)) {
        throw new AssertionError(
            String.format(
                "Entity %s missing expected subType '%s'. Actual subTypes: %s",
                urn, expectedSubType, actualSubTypes));
      }
    }
  }
}
