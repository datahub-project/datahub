package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import java.io.IOException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for IndexRoleUtils class.
 *
 * <p>This test class covers all public methods in IndexRoleUtils, including:
 *
 * <ul>
 *   <li>Elasticsearch Cloud role and user creation
 *   <li>AWS OpenSearch role and user creation
 *   <li>Retry logic for transient failures
 *   <li>Proper handling of existing resources (409 conflicts)
 *   <li>Error scenarios and exception handling
 * </ul>
 */
public class IndexRoleUtilsTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  @Mock private SearchClientShim searchClient;
  @Mock private RawResponse rawResponse;
  @Mock private ResponseException responseException;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(esComponents.getSearchClient()).thenReturn(searchClient);
  }

  // ==================== Elasticsearch Cloud Role Tests ====================

  @Test
  public void testCreateElasticsearchCloudRole_Success() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateElasticsearchCloudRole_AlreadyExists() throws IOException {
    // Arrange
    String roleName = "existing_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateElasticsearchCloudRole_ResponseException409() throws IOException {
    // Arrange
    String roleName = "existing_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateElasticsearchCloudRole_ResponseException500() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateElasticsearchCloudRole_RetryFailure() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    // Mock 5 consecutive failures to trigger retry exhaustion
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(400, "Bad Request"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);
  }

  // ==================== Elasticsearch Cloud User Tests ====================

  @Test
  public void testCreateElasticsearchCloudUser_Success() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";
    String prefix = "test_";

    // Mock successful role creation
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createElasticsearchCloudUser(esComponents, roleName, username, password, prefix);

    // Assert
    // Should make 2 calls: one for role creation, one for user creation
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateElasticsearchCloudUser_RoleAlreadyExists() throws IOException {
    // Arrange
    String roleName = "existing_role";
    String username = "test_user";
    String password = "test_password";
    String prefix = "test_";

    // Mock role already exists (409), then successful user creation
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse)
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(createStatusLine(409, "Conflict"))
        .thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createElasticsearchCloudUser(esComponents, roleName, username, password, prefix);

    // Assert
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateElasticsearchCloudUser_RoleCreationFails() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";
    String prefix = "test_";

    // Mock role creation failure
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(400, "Bad Request"));

    // Act
    IndexRoleUtils.createElasticsearchCloudUser(esComponents, roleName, username, password, prefix);
  }

  // ==================== AWS OpenSearch Role Tests ====================

  @Test
  public void testCreateAwsOpenSearchRole_Success() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRole(esComponents, roleName, prefix);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateAwsOpenSearchRole_AlreadyExists() throws IOException {
    // Arrange
    String roleName = "existing_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRole(esComponents, roleName, prefix);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateAwsOpenSearchRole_ResponseException409() throws IOException {
    // Arrange
    String roleName = "existing_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRole(esComponents, roleName, prefix);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchRole_ResponseException500() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRole(esComponents, roleName, prefix);
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchRole_RetryFailure() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    // Mock 5 consecutive failures to trigger retry exhaustion
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(400, "Bad Request"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRole(esComponents, roleName, prefix);
  }

  // ==================== AWS OpenSearch User Tests ====================

  @Test
  public void testCreateAwsOpenSearchUser_Success() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";
    String prefix = "test_";

    // Mock successful role creation
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(esComponents, roleName, username, password, prefix);

    // Assert
    // Should make 2 calls: one for role creation, one for user creation
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateAwsOpenSearchUser_RoleAlreadyExists() throws IOException {
    // Arrange
    String roleName = "existing_role";
    String username = "test_user";
    String password = "test_password";
    String prefix = "test_";

    // Mock role already exists (409), then successful user creation
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse)
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(createStatusLine(409, "Conflict"))
        .thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(esComponents, roleName, username, password, prefix);

    // Assert
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchUser_RoleCreationFails() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";
    String prefix = "test_";

    // Mock role creation failure
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(400, "Bad Request"));

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(esComponents, roleName, username, password, prefix);
  }

  // ==================== Retry Logic Tests ====================

  @Test
  public void testRetryLogic_TransientFailureThenSuccess() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    // Mock transient failure (400) followed by success (200)
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse)
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(createStatusLine(400, "Bad Request"))
        .thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);

    // Assert
    // Should retry once and succeed on second attempt
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testRetryLogic_TransientFailureThenConflict() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    // Mock transient failure (400) followed by conflict (409)
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse)
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(createStatusLine(400, "Bad Request"))
        .thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);

    // Assert
    // Should retry once and succeed on second attempt (409 is considered success)
    Mockito.verify(searchClient, Mockito.times(2))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  // ==================== Endpoint Verification Tests ====================

  @Test
  public void testElasticsearchCloudRoleEndpoint() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);

    // Assert
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request.getMethod().equals("PUT")
                        && request.getEndpoint().equals("/_security/role/" + roleName)));
  }

  @Test
  public void testAwsOpenSearchRoleEndpoint() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRole(esComponents, roleName, prefix);

    // Assert
    Mockito.verify(searchClient)
        .performLowLevelRequest(
            Mockito.argThat(
                request ->
                    request.getMethod().equals("PUT")
                        && request
                            .getEndpoint()
                            .equals("/_opendistro/_security/api/roles/" + roleName)));
  }

  @Test
  public void testCreateElasticsearchCloudRole_OuterResponseException409() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    // Mock ResponseException with 409 status in the outer catch block
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);

    // Assert - Should handle 409 gracefully without throwing exception
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateElasticsearchCloudRole_OuterResponseException500() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    // Mock ResponseException with 500 status in the outer catch block
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));

    // Act
    IndexRoleUtils.createElasticsearchCloudRole(esComponents, roleName, prefix);
  }

  @Test
  public void testCreateElasticsearchCloudUserInternal_OuterResponseException409()
      throws IOException {
    // Arrange
    String username = "test_user";
    String password = "test_password";
    String roleName = "test_role";

    // Mock ResponseException with 409 status in the outer catch block
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createElasticsearchCloudUser(
        esComponents, roleName, username, password, "test_");

    // Assert - Should handle 409 gracefully without throwing exception
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateElasticsearchCloudUserInternal_OuterResponseException500()
      throws IOException {
    // Arrange
    String username = "test_user";
    String password = "test_password";
    String roleName = "test_role";

    // Mock ResponseException with 500 status in the outer catch block
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));

    // Act
    IndexRoleUtils.createElasticsearchCloudUser(
        esComponents, roleName, username, password, "test_");
  }

  @Test
  public void testCreateAwsOpenSearchRole_OuterResponseException409() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    // Mock ResponseException with 409 status in the outer catch block
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRole(esComponents, roleName, prefix);

    // Assert - Should handle 409 gracefully without throwing exception
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchRole_OuterResponseException500() throws IOException {
    // Arrange
    String roleName = "test_role";
    String prefix = "test_";

    // Mock ResponseException with 500 status in the outer catch block
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRole(esComponents, roleName, prefix);
  }

  @Test
  public void testCreateAwsOpenSearchUserInternal_OuterResponseException409() throws IOException {
    // Arrange
    String username = "test_user";
    String password = "test_password";
    String roleName = "test_role";

    // Mock ResponseException with 409 status in the outer catch block
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(esComponents, roleName, username, password, "test_");

    // Assert - Should handle 409 gracefully without throwing exception
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchUserInternal_OuterResponseException500() throws IOException {
    // Arrange
    String username = "test_user";
    String password = "test_password";
    String roleName = "test_role";

    // Mock ResponseException with 500 status in the outer catch block
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(esComponents, roleName, username, password, "test_");
  }

  // ==================== Helper Methods ====================

  /**
   * Creates a mock StatusLine with the specified status code and reason phrase.
   *
   * @param statusCode the HTTP status code
   * @param reasonPhrase the HTTP reason phrase
   * @return a mock StatusLine
   */
  private org.apache.http.StatusLine createStatusLine(int statusCode, String reasonPhrase) {
    return new org.apache.http.StatusLine() {
      @Override
      public int getStatusCode() {
        return statusCode;
      }

      @Override
      public String getReasonPhrase() {
        return reasonPhrase;
      }

      @Override
      public org.apache.http.ProtocolVersion getProtocolVersion() {
        return new org.apache.http.ProtocolVersion("HTTP", 1, 1);
      }
    };
  }
}
