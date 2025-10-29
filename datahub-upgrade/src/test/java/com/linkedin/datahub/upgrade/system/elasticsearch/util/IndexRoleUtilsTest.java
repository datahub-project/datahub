package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
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

  private OperationContext operationContext = TestOperationContexts.systemContextNoValidate();

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

    // Mock successful user creation only (role should be created separately)
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK")); // PUT user

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, null, operationContext);

    // Assert
    // Should make calls for user creation
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateAwsOpenSearchUser_AlreadyExists() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";

    // Mock user already exists (409)
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine())
        .thenReturn(createStatusLine(409, "Conflict")); // PUT user - already exists

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, null, operationContext);

    // Assert
    // User creation attempted
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  // ==================== AWS OpenSearch Role Mapping Tests ====================

  @Test
  public void testCreateAwsOpenSearchRoleMapping_Success() throws IOException {
    // Arrange
    String roleName = "test_role";
    String iamRoleArn = "arn:aws:iam::123456789012:role/test-role";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRoleMapping(esComponents, roleName, iamRoleArn);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateAwsOpenSearchRoleMapping_AlreadyExists() throws IOException {
    // Arrange
    String roleName = "existing_role";
    String iamRoleArn = "arn:aws:iam::123456789012:role/test-role";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(409, "Conflict"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRoleMapping(esComponents, roleName, iamRoleArn);

    // Assert
    Mockito.verify(searchClient).performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchRoleMapping_ResponseException500() throws IOException {
    // Arrange
    String roleName = "test_role";
    String iamRoleArn = "arn:aws:iam::123456789012:role/test-role";

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse())
        .thenReturn(Mockito.mock(org.opensearch.client.Response.class));
    Mockito.when(responseException.getResponse().getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));

    // Act
    IndexRoleUtils.createAwsOpenSearchRoleMapping(esComponents, roleName, iamRoleArn);
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
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, null, operationContext);

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
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, null, operationContext);
  }

  // ==================== IAM-Only Authentication Tests ====================

  @Test
  public void testCreateAwsOpenSearchUser_IamOnlyAuth_Success() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = null; // IAM-only, no password
    String iamRoleArn = "arn:aws:iam::123456789012:role/test-role";
    String prefix = "test_";

    // Mock successful user creation only (role should be created separately)
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK")); // PUT user

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, iamRoleArn, operationContext);

    // Assert
    // Should make calls for user creation
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateAwsOpenSearchUser_PasswordBasedAuth_Success() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";
    String iamRoleArn = null; // No IAM role, password-based
    String prefix = "test_";

    // Mock successful user creation only (role should be created separately)
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK")); // PUT user

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, iamRoleArn, operationContext);

    // Assert
    // Should make calls for user creation
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  @Test
  public void testCreateAwsOpenSearchUser_IamOnlyAuth_EmptyIamRole() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = null;
    String iamRoleArn = ""; // Empty IAM role
    String prefix = "test_";

    // Mock successful user creation only (role should be created separately)
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(200, "OK")); // PUT user

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, iamRoleArn, operationContext);

    // Assert
    // Should make calls for user creation
    Mockito.verify(searchClient, Mockito.atLeast(1))
        .performLowLevelRequest(Mockito.any(Request.class));
  }

  // ==================== AWS OpenSearch User Error Logging Tests ====================

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchUser_ErrorWithResponseBody() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";

    // Mock error response with response body
    org.apache.http.HttpEntity mockEntity = Mockito.mock(org.apache.http.HttpEntity.class);

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(400, "Bad Request"));
    Mockito.when(rawResponse.getEntity()).thenReturn(mockEntity);

    try {
      Mockito.when(mockEntity.getContent())
          .thenReturn(
              new java.io.ByteArrayInputStream("{\"error\":\"Invalid credentials\"}".getBytes()));
    } catch (Exception e) {
      // Handle exception
    }

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, null, operationContext);
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchUser_ResponseExceptionWithResponseBody() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";

    // Mock ResponseException with response body
    org.opensearch.client.Response mockResponse =
        Mockito.mock(org.opensearch.client.Response.class);
    org.apache.http.HttpEntity mockEntity = Mockito.mock(org.apache.http.HttpEntity.class);

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse()).thenReturn(mockResponse);
    Mockito.when(mockResponse.getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));
    Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);

    try {
      Mockito.when(mockEntity.getContent())
          .thenReturn(new java.io.ByteArrayInputStream("{\"error\":\"Server error\"}".getBytes()));
    } catch (Exception e) {
      // Handle exception
    }

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, null, operationContext);
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchUser_RetryableErrorThenFailure() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";

    org.apache.http.HttpEntity mockEntity = Mockito.mock(org.apache.http.HttpEntity.class);

    // Mock retryable error (400) returned repeatedly
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(400, "Bad Request"));
    Mockito.when(rawResponse.getEntity()).thenReturn(mockEntity);

    try {
      Mockito.when(mockEntity.getContent())
          .thenReturn(
              new java.io.ByteArrayInputStream("{\"error\":\"Validation failed\"}".getBytes()));
    } catch (Exception e) {
      // Handle exception
    }

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, null, operationContext);
  }

  // ==================== AWS OpenSearch Role Mapping Error Logging Tests ====================

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchRoleMapping_ErrorWithResponseBody() throws IOException {
    // Arrange
    String roleName = "test_role";
    String iamRoleArn = "arn:aws:iam::123456789012:role/test-role";

    // Mock error response with response body
    org.apache.http.HttpEntity mockEntity = Mockito.mock(org.apache.http.HttpEntity.class);

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(400, "Bad Request"));
    Mockito.when(rawResponse.getEntity()).thenReturn(mockEntity);

    try {
      Mockito.when(mockEntity.getContent())
          .thenReturn(
              new java.io.ByteArrayInputStream("{\"error\":\"Invalid IAM role\"}".getBytes()));
    } catch (Exception e) {
      // Handle exception
    }

    // Act
    IndexRoleUtils.createAwsOpenSearchRoleMapping(esComponents, roleName, iamRoleArn);
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchRoleMapping_ResponseExceptionWithResponseBody()
      throws IOException {
    // Arrange
    String roleName = "test_role";
    String iamRoleArn = "arn:aws:iam::123456789012:role/test-role";

    // Mock ResponseException with response body
    org.opensearch.client.Response mockResponse =
        Mockito.mock(org.opensearch.client.Response.class);
    org.apache.http.HttpEntity mockEntity = Mockito.mock(org.apache.http.HttpEntity.class);

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenThrow(responseException);
    Mockito.when(responseException.getResponse()).thenReturn(mockResponse);
    Mockito.when(mockResponse.getStatusLine())
        .thenReturn(createStatusLine(500, "Internal Server Error"));
    Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);

    try {
      Mockito.when(mockEntity.getContent())
          .thenReturn(new java.io.ByteArrayInputStream("{\"error\":\"Mapping error\"}".getBytes()));
    } catch (Exception e) {
      // Handle exception
    }

    // Act
    IndexRoleUtils.createAwsOpenSearchRoleMapping(esComponents, roleName, iamRoleArn);
  }

  // ==================== Response Body Extraction Tests ====================

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchUser_NoEntityReturnsNoResponseBody() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";

    // Mock error response without entity
    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(400, "Bad Request"));
    // No entity set, so extractResponseBody should return "No response body"

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, null, operationContext);
  }

  @Test(expectedExceptions = IOException.class)
  public void testCreateAwsOpenSearchUser_EntityReadException() throws IOException {
    // Arrange
    String roleName = "test_role";
    String username = "test_user";
    String password = "test_password";

    // Mock error response with entity that throws exception on read
    org.apache.http.HttpEntity mockEntity = Mockito.mock(org.apache.http.HttpEntity.class);

    Mockito.when(searchClient.performLowLevelRequest(Mockito.any(Request.class)))
        .thenReturn(rawResponse);
    Mockito.when(rawResponse.getStatusLine()).thenReturn(createStatusLine(400, "Bad Request"));
    Mockito.when(rawResponse.getEntity()).thenReturn(mockEntity);

    try {
      // Entity throws exception when reading
      Mockito.when(mockEntity.getContent()).thenThrow(new IOException("Cannot read entity"));
    } catch (IOException e) {
      // Expected
    }

    // Act
    IndexRoleUtils.createAwsOpenSearchUser(
        esComponents, username, password, roleName, null, operationContext);
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
