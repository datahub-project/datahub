package client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AuthServiceClientTest {

  @Mock private CloseableHttpClient mockHttpClient;
  @Mock private CloseableHttpResponse mockResponse;
  @Mock private HttpEntity mockEntity;
  @Mock private StatusLine mockStatusLine;

  private AuthServiceClient authServiceClient;
  private Authentication systemAuthentication;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Create system authentication
    Actor systemActor = new Actor(ActorType.USER, "datahub");
    systemAuthentication =
        new Authentication(systemActor, "Basic datahub:datahub", java.util.Collections.emptyMap());

    // Create AuthServiceClient with test configuration
    authServiceClient =
        new AuthServiceClient(
            "localhost",
            8080,
            "/api/v2", // basePath
            false, // useSsl
            systemAuthentication,
            mockHttpClient);
  }

  @Test
  public void testConstructorWithBasePath() {
    // Test constructor with basePath
    AuthServiceClient client =
        new AuthServiceClient(
            "localhost", 8080, "/datahub", true, systemAuthentication, mockHttpClient);

    assertNotNull(client);
  }

  @Test
  public void testConstructorWithEmptyBasePath() {
    // Test constructor with empty basePath
    AuthServiceClient client =
        new AuthServiceClient("localhost", 8080, "", false, systemAuthentication, mockHttpClient);

    assertNotNull(client);
  }

  @Test
  public void testConstructorWithNullParameters() {
    // Test constructor with null parameters should throw exceptions
    assertThrows(
        NullPointerException.class,
        () -> {
          new AuthServiceClient(null, 8080, "", false, systemAuthentication, mockHttpClient);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new AuthServiceClient("localhost", null, "", false, systemAuthentication, mockHttpClient);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new AuthServiceClient(
              "localhost", 8080, null, false, systemAuthentication, mockHttpClient);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new AuthServiceClient("localhost", 8080, "", null, systemAuthentication, mockHttpClient);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new AuthServiceClient("localhost", 8080, "", false, null, mockHttpClient);
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          new AuthServiceClient("localhost", 8080, "", false, systemAuthentication, null);
        });
  }

  @Test
  public void testGenerateSessionTokenForUserWithBasePath() throws Exception {
    // Test generateSessionTokenForUser with basePath
    String userId = "testuser";
    String loginSource = "PASSWORD_LOGIN";
    String expectedToken = "test-access-token";

    // Mock response
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream(createTokenResponseJson(expectedToken).getBytes()));

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute the method
    String result = authServiceClient.generateSessionTokenForUser(userId, loginSource);

    // Verify the result
    assertEquals(expectedToken, result);

    // Verify the HTTP request was made
    verify(mockHttpClient).execute(any(HttpPost.class));
  }

  @Test
  public void testGenerateSessionTokenForUserWithEmptyBasePath() throws Exception {
    // Test generateSessionTokenForUser with empty basePath
    AuthServiceClient client =
        new AuthServiceClient("localhost", 8080, "", false, systemAuthentication, mockHttpClient);

    String userId = "testuser";
    String loginSource = "PASSWORD_LOGIN";
    String expectedToken = "test-access-token";

    // Mock response
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream(createTokenResponseJson(expectedToken).getBytes()));

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute the method
    String result = client.generateSessionTokenForUser(userId, loginSource);

    // Verify the result
    assertEquals(expectedToken, result);

    // Verify the HTTP request was made with correct URL without basePath
    verify(mockHttpClient)
        .execute(
            argThat(
                request -> {
                  if (request instanceof HttpPost) {
                    HttpPost postRequest = (HttpPost) request;
                    String uri = postRequest.getURI().toString();
                    return uri.contains("/auth/generateSessionTokenForUser")
                        && !uri.contains("/api/v2");
                  }
                  return false;
                }));
  }

  @Test
  public void testGenerateSessionTokenForUserWithHttps() throws Exception {
    // Test generateSessionTokenForUser with HTTPS
    AuthServiceClient client =
        new AuthServiceClient(
            "localhost",
            8443,
            "/api/v2",
            true, // useSsl
            systemAuthentication,
            mockHttpClient);

    String userId = "testuser";
    String loginSource = "PASSWORD_LOGIN";
    String expectedToken = "test-access-token";

    // Mock response
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream(createTokenResponseJson(expectedToken).getBytes()));

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute the method
    String result = client.generateSessionTokenForUser(userId, loginSource);

    // Verify the result
    assertEquals(expectedToken, result);

    // Verify the HTTP request was made with HTTPS URL
    verify(mockHttpClient)
        .execute(
            argThat(
                request -> {
                  if (request instanceof HttpPost) {
                    HttpPost postRequest = (HttpPost) request;
                    String uri = postRequest.getURI().toString();
                    return uri.startsWith("https://") && uri.contains("8443");
                  }
                  return false;
                }));
  }

  @Test
  public void testGenerateSessionTokenForUserWithNullUserId() {
    // Test generateSessionTokenForUser with null userId
    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.generateSessionTokenForUser(null, "PASSWORD_LOGIN");
        });
  }

  @Test
  public void testGenerateSessionTokenForUserWithBadResponse() throws Exception {
    // Test generateSessionTokenForUser with bad response
    String userId = "testuser";
    String loginSource = "PASSWORD_LOGIN";

    // Mock bad response - need to mock getContent() for error reading
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_BAD_REQUEST);
    when(mockStatusLine.toString()).thenReturn("HTTP/1.1 400 Bad Request");
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.toString()).thenReturn("Bad Request Entity");
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream("Bad Request Body".getBytes()));

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute the method and expect exception
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              authServiceClient.generateSessionTokenForUser(userId, loginSource);
            });

    // The outer exception wraps the inner one, so check for the wrapper message
    assertTrue(
        exception.getMessage().contains("Failed to generate session token for user"),
        "Exception message was: " + exception.getMessage());

    // Verify the cause contains the bad response message
    assertNotNull(exception.getCause());
    assertTrue(
        exception.getCause().getMessage().contains("Bad response from the Metadata Service"),
        "Cause message was: " + exception.getCause().getMessage());
  }

  @Test
  public void testSignUpWithBasePath() throws Exception {
    // Test signUp with basePath
    String userUrn = "urn:li:corpuser:testuser";
    String fullName = "Test User";
    String email = "test@example.com";
    String title = "Software Engineer";
    String password = "password123";
    String inviteToken = "invite-token-123";

    // Mock response
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream(createSignUpResponseJson(true).getBytes()));

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute the method
    boolean result =
        authServiceClient.signUp(userUrn, fullName, email, title, password, inviteToken);

    // Verify the result
    assertTrue(result);

    // Verify the HTTP request was made with correct URL including basePath
    verify(mockHttpClient)
        .execute(
            argThat(
                request -> {
                  if (request instanceof HttpPost) {
                    HttpPost postRequest = (HttpPost) request;
                    String uri = postRequest.getURI().toString();
                    return uri.contains("/api/v2/auth/signUp");
                  }
                  return false;
                }));
  }

  @Test
  public void testSignUpWithNullParameters() {
    // Test signUp with null parameters
    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.signUp(
              null, "Test User", "test@example.com", "Engineer", "password", "token");
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.signUp(
              "urn:li:corpuser:test", null, "test@example.com", "Engineer", "password", "token");
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.signUp(
              "urn:li:corpuser:test", "Test User", null, "Engineer", "password", "token");
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.signUp(
              "urn:li:corpuser:test", "Test User", "test@example.com", "Engineer", null, "token");
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.signUp(
              "urn:li:corpuser:test",
              "Test User",
              "test@example.com",
              "Engineer",
              "password",
              null);
        });
  }

  @Test
  public void testResetNativeUserCredentialsWithBasePath() throws Exception {
    // Test resetNativeUserCredentials with basePath
    String userUrn = "urn:li:corpuser:testuser";
    String password = "newpassword123";
    String resetToken = "reset-token-123";

    // Mock response
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream(createResetCredentialsResponseJson(true).getBytes()));

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute the method
    boolean result = authServiceClient.resetNativeUserCredentials(userUrn, password, resetToken);

    // Verify the result
    assertTrue(result);

    // Verify the HTTP request was made with correct URL including basePath
    verify(mockHttpClient)
        .execute(
            argThat(
                request -> {
                  if (request instanceof HttpPost) {
                    HttpPost postRequest = (HttpPost) request;
                    String uri = postRequest.getURI().toString();
                    return uri.contains("/api/v2/auth/resetNativeUserCredentials");
                  }
                  return false;
                }));
  }

  @Test
  public void testResetNativeUserCredentialsWithNullParameters() {
    // Test resetNativeUserCredentials with null parameters
    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.resetNativeUserCredentials(null, "password", "token");
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.resetNativeUserCredentials("urn:li:corpuser:test", null, "token");
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.resetNativeUserCredentials("urn:li:corpuser:test", "password", null);
        });
  }

  @Test
  public void testVerifyNativeUserCredentialsWithBasePath() throws Exception {
    // Test verifyNativeUserCredentials with basePath
    String userUrn = "urn:li:corpuser:testuser";
    String password = "password123";

    // Mock response
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream(createVerifyCredentialsResponseJson(true).getBytes()));

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute the method
    boolean result = authServiceClient.verifyNativeUserCredentials(userUrn, password);

    // Verify the result
    assertTrue(result);

    // Verify the HTTP request was made with correct URL including basePath
    verify(mockHttpClient)
        .execute(
            argThat(
                request -> {
                  if (request instanceof HttpPost) {
                    HttpPost postRequest = (HttpPost) request;
                    String uri = postRequest.getURI().toString();
                    return uri.contains("/api/v2/auth/verifyNativeUserCredentials");
                  }
                  return false;
                }));
  }

  @Test
  public void testVerifyNativeUserCredentialsWithNullParameters() {
    // Test verifyNativeUserCredentials with null parameters
    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.verifyNativeUserCredentials(null, "password");
        });

    assertThrows(
        NullPointerException.class,
        () -> {
          authServiceClient.verifyNativeUserCredentials("urn:li:corpuser:test", null);
        });
  }

  @Test
  public void testGenerateSessionTokenForUserWithIOException() throws Exception {
    // Test generateSessionTokenForUser with IOException
    String userId = "testuser";
    String loginSource = "PASSWORD_LOGIN";

    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenThrow(new IOException("Connection failed"));

    // Execute the method and expect exception
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              authServiceClient.generateSessionTokenForUser(userId, loginSource);
            });

    assertTrue(exception.getMessage().contains("Failed to generate session token for user"));
    assertTrue(exception.getCause() instanceof IOException);
  }

  @Test
  public void testSignUpWithIOException() throws Exception {
    // Test signUp with IOException
    String userUrn = "urn:li:corpuser:testuser";
    String fullName = "Test User";
    String email = "test@example.com";
    String title = "Software Engineer";
    String password = "password123";
    String inviteToken = "invite-token-123";

    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenThrow(new IOException("Connection failed"));

    // Execute the method and expect exception
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              authServiceClient.signUp(userUrn, fullName, email, title, password, inviteToken);
            });

    assertTrue(exception.getMessage().contains("Failed to create user"));
    assertTrue(exception.getCause() instanceof IOException);
  }

  @Test
  public void testJsonParsingWithInvalidJson() throws Exception {
    // Test JSON parsing with invalid JSON
    String userId = "testuser";
    String loginSource = "PASSWORD_LOGIN";

    // Mock response with invalid JSON
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream("invalid json".getBytes()));

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute the method and expect exception (wrapped in RuntimeException or direct)
    Throwable thrown =
        assertThrows(
            Throwable.class,
            () -> authServiceClient.generateSessionTokenForUser(userId, loginSource));

    IllegalArgumentException parseError =
        thrown instanceof RuntimeException && thrown.getCause() instanceof IllegalArgumentException
            ? (IllegalArgumentException) thrown.getCause()
            : thrown instanceof IllegalArgumentException ? (IllegalArgumentException) thrown : null;
    assertNotNull(
        parseError,
        "Expected RuntimeException with IllegalArgumentException cause or IllegalArgumentException");
    assertTrue(
        parseError.getMessage().contains("Failed to parse JSON received from the MetadataService"));
  }

  @Test
  public void testJsonParsingWithMissingField() throws Exception {
    // Test JSON parsing with missing field
    String userId = "testuser";
    String loginSource = "PASSWORD_LOGIN";

    // Mock response with JSON missing accessToken field
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockEntity.getContent())
        .thenReturn(new ByteArrayInputStream("{\"userId\":\"testuser\"}".getBytes()));

    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    // Execute the method and expect exception (wrapped in RuntimeException or direct)
    Throwable thrown =
        assertThrows(
            Throwable.class,
            () -> authServiceClient.generateSessionTokenForUser(userId, loginSource));

    IllegalArgumentException parseError =
        thrown instanceof RuntimeException && thrown.getCause() instanceof IllegalArgumentException
            ? (IllegalArgumentException) thrown.getCause()
            : thrown instanceof IllegalArgumentException ? (IllegalArgumentException) thrown : null;
    assertNotNull(
        parseError,
        "Expected RuntimeException with IllegalArgumentException cause or IllegalArgumentException");
    assertTrue(
        parseError.getMessage().contains("Failed to parse JSON received from the MetadataService"));
  }

  // Helper methods to create JSON responses
  private String createTokenResponseJson(String accessToken) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    node.put("accessToken", accessToken);
    return node.toString();
  }

  private String createSignUpResponseJson(boolean isCreated) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    node.put("isNativeUserCreated", isCreated);
    return node.toString();
  }

  private String createResetCredentialsResponseJson(boolean isReset) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    node.put("areNativeUserCredentialsReset", isReset);
    return node.toString();
  }

  private String createVerifyCredentialsResponseJson(boolean doesMatch) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    node.put("doesPasswordMatch", doesMatch);
    return node.toString();
  }
}
