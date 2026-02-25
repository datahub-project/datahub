package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexRoleUtils;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.function.Function;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateUserStepTest {

  @Mock private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  @Mock private ConfigurationProvider configurationProvider;
  @Mock private SearchClientShim searchClient;
  @Mock private SearchClientShim.SearchEngineType searchEngineType;
  @Mock private UpgradeContext upgradeContext;
  @Mock private ElasticSearchConfiguration elasticSearch;
  @Mock private IndexConfiguration index;

  private CreateUserStep step;
  private OperationContext opContext = TestOperationContexts.systemContextNoValidate();

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Setup common mocks
    Mockito.when(esComponents.getSearchClient()).thenReturn(searchClient);
    Mockito.when(searchClient.getEngineType()).thenReturn(searchEngineType);
    Mockito.when(configurationProvider.getElasticSearch()).thenReturn(elasticSearch);
    Mockito.when(elasticSearch.getIndex()).thenReturn(index);

    Mockito.when(upgradeContext.opContext()).thenReturn(opContext);

    step = new CreateUserStep(esComponents, configurationProvider);
  }

  @AfterMethod
  public void tearDown() {
    // Clear environment variables after each test
    System.clearProperty("CREATE_USER_ES");
    System.clearProperty("CREATE_USER_ES_USERNAME");
    System.clearProperty("CREATE_USER_ES_PASSWORD");
    System.clearProperty("CREATE_USER_ES_IAM_ROLE_ARN");
  }

  @Test
  public void testId() {
    // Act
    String id = step.id();

    // Assert
    Assert.assertEquals(id, "CreateElasticsearchUserStep");
  }

  @Test
  public void testRetryCount() {
    // Act
    int retryCount = step.retryCount();

    // Assert
    Assert.assertEquals(retryCount, 3);
  }

  @Test
  public void testSkip_WhenCreateUserEsIsFalse() {
    // Arrange
    System.setProperty("CREATE_USER_ES", "false");

    // Act
    boolean shouldSkip = step.skip(upgradeContext);

    // Assert
    Assert.assertTrue(shouldSkip);
  }

  @Test
  public void testSkip_WhenCreateUserEsIsTrue() {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");

    // Act
    boolean shouldSkip = step.skip(upgradeContext);

    // Assert
    Assert.assertFalse(shouldSkip);
  }

  @Test
  public void testSkip_WhenCreateUserEsIsNotSet() {
    // Arrange - no environment variable set

    // Act
    boolean shouldSkip = step.skip(upgradeContext);

    // Assert
    Assert.assertTrue(shouldSkip); // Should skip by default
  }

  @Test
  public void testExecutable_OpenSearchPath_Success() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    System.setProperty("CREATE_USER_ES_PASSWORD", "testpass");
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(true);

    try (MockedStatic<IndexUtils> indexUtilsMock = Mockito.mockStatic(IndexUtils.class);
        MockedStatic<IndexRoleUtils> indexRoleUtilsMock =
            Mockito.mockStatic(IndexRoleUtils.class)) {

      indexUtilsMock.when(() -> IndexUtils.isAwsOpenSearchService(esComponents)).thenReturn(true);
      indexRoleUtilsMock
          .when(
              () ->
                  IndexRoleUtils.createAwsOpenSearchRole(
                      Mockito.any(), Mockito.anyString(), Mockito.anyString()))
          .thenAnswer(invocation -> null);
      indexRoleUtilsMock
          .when(
              () ->
                  IndexRoleUtils.createAwsOpenSearchUser(
                      Mockito.any(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.any()))
          .thenAnswer(invocation -> null);

      // Act
      Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
      UpgradeStepResult result = executable.apply(upgradeContext);

      // Assert
      Assert.assertNotNull(result);
      Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
      Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

      // Verify OpenSearch path was taken
      Mockito.verify(searchEngineType).isOpenSearch();
      Mockito.verify(index).getFinalPrefix();
    }
  }

  @Test
  public void testExecutable_ElasticsearchPath_Success() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    System.setProperty("CREATE_USER_ES_PASSWORD", "testpass");
    Mockito.when(index.getFinalPrefix()).thenReturn("prod_");
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);

    try (MockedStatic<IndexRoleUtils> indexRoleUtilsMock =
        Mockito.mockStatic(IndexRoleUtils.class)) {

      indexRoleUtilsMock
          .when(
              () ->
                  IndexRoleUtils.createElasticsearchCloudUser(
                      Mockito.any(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString()))
          .thenAnswer(invocation -> null);

      // Act
      Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
      UpgradeStepResult result = executable.apply(upgradeContext);

      // Assert
      Assert.assertNotNull(result);
      Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
      Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

      // Verify Elasticsearch path was taken
      Mockito.verify(searchEngineType).isOpenSearch();
      Mockito.verify(index).getFinalPrefix();
    }
  }

  @Test
  public void testExecutable_MissingUsername() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_PASSWORD", "testpass");
    // Username not set
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_MissingPassword() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    // Password not set
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_MissingBothCredentials() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    // Neither username nor password set
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_OpenSearchPath_Exception() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    System.setProperty("CREATE_USER_ES_PASSWORD", "testpass");
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(true);
    Mockito.when(index.getFinalPrefix()).thenThrow(new RuntimeException("OpenSearch setup failed"));

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_ElasticsearchPath_Exception() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    System.setProperty("CREATE_USER_ES_PASSWORD", "testpass");
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);
    Mockito.when(index.getFinalPrefix())
        .thenThrow(new RuntimeException("Elasticsearch setup failed"));

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_WithEmptyPrefix() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    System.setProperty("CREATE_USER_ES_PASSWORD", "testpass");
    Mockito.when(index.getFinalPrefix()).thenReturn(""); // Empty prefix
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);

    try (MockedStatic<IndexRoleUtils> indexRoleUtilsMock =
        Mockito.mockStatic(IndexRoleUtils.class)) {

      indexRoleUtilsMock
          .when(
              () ->
                  IndexRoleUtils.createElasticsearchCloudUser(
                      Mockito.any(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString()))
          .thenAnswer(invocation -> null);

      // Act
      Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
      UpgradeStepResult result = executable.apply(upgradeContext);

      // Assert
      Assert.assertNotNull(result);
      Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
      Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

      // Verify empty prefix was used
      Mockito.verify(index).getFinalPrefix();
    }
  }

  @Test
  public void testExecutable_WithNullPrefix() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    System.setProperty("CREATE_USER_ES_PASSWORD", "testpass");
    Mockito.when(index.getFinalPrefix()).thenReturn(null); // Null prefix
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);

    try (MockedStatic<IndexRoleUtils> indexRoleUtilsMock =
        Mockito.mockStatic(IndexRoleUtils.class)) {

      indexRoleUtilsMock
          .when(
              () ->
                  IndexRoleUtils.createElasticsearchCloudUser(
                      Mockito.any(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString()))
          .thenAnswer(invocation -> null);

      // Act
      Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
      UpgradeStepResult result = executable.apply(upgradeContext);

      // Assert
      Assert.assertNotNull(result);
      Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
      Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

      // Verify null prefix was handled
      Mockito.verify(index).getFinalPrefix();
    }
  }

  @Test
  public void testExecutable_MultipleCalls() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    System.setProperty("CREATE_USER_ES_PASSWORD", "testpass");
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(false);

    try (MockedStatic<IndexRoleUtils> indexRoleUtilsMock =
        Mockito.mockStatic(IndexRoleUtils.class)) {

      indexRoleUtilsMock
          .when(
              () ->
                  IndexRoleUtils.createElasticsearchCloudUser(
                      Mockito.any(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString(),
                      Mockito.anyString()))
          .thenAnswer(invocation -> null);

      // Act
      Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
      UpgradeStepResult result1 = executable.apply(upgradeContext);
      UpgradeStepResult result2 = executable.apply(upgradeContext);

      // Assert
      Assert.assertNotNull(result1);
      Assert.assertNotNull(result2);
      Assert.assertEquals(result1.result(), DataHubUpgradeState.SUCCEEDED);
      Assert.assertEquals(result2.result(), DataHubUpgradeState.SUCCEEDED);

      // Verify that the executable function can be called multiple times
      Mockito.verify(searchEngineType, Mockito.times(2)).isOpenSearch();
      Mockito.verify(index, Mockito.times(2)).getFinalPrefix();
    }
  }

  @Test
  public void testExecutable_IamOnlyAuth_Success() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_IAM_ROLE_ARN", "arn:aws:iam::123456789012:role/test-role");
    // No username/password - IAM-only authentication
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");
    Mockito.when(searchEngineType.isOpenSearch()).thenReturn(true);

    try (MockedStatic<IndexUtils> indexUtilsMock = Mockito.mockStatic(IndexUtils.class);
        MockedStatic<IndexRoleUtils> indexRoleUtilsMock =
            Mockito.mockStatic(IndexRoleUtils.class)) {

      indexUtilsMock.when(() -> IndexUtils.isAwsOpenSearchService(esComponents)).thenReturn(true);
      indexRoleUtilsMock
          .when(
              () ->
                  IndexRoleUtils.createAwsOpenSearchRole(
                      Mockito.any(), Mockito.anyString(), Mockito.anyString()))
          .thenAnswer(invocation -> null);
      indexRoleUtilsMock
          .when(
              () ->
                  IndexRoleUtils.createAwsOpenSearchRoleMapping(
                      Mockito.any(), Mockito.anyString(), Mockito.anyString()))
          .thenAnswer(invocation -> null);

      // Act
      Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
      UpgradeStepResult result = executable.apply(upgradeContext);

      // Assert
      Assert.assertNotNull(result);
      Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
      Assert.assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

      // Verify OpenSearch path was taken
      Mockito.verify(searchEngineType).isOpenSearch();
      Mockito.verify(index).getFinalPrefix();
    }
  }

  @Test
  public void testExecutable_MissingUsernameWithIamRole() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_IAM_ROLE_ARN", "arn:aws:iam::123456789012:role/test-role");
    // Username not set - should fail
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_MissingPasswordAndIamRole() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    // Neither password nor IAM role set - should fail
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExecutable_IamOnlyAuth_EmptyIamRole() throws Exception {
    // Arrange
    System.setProperty("CREATE_USER_ES", "true");
    System.setProperty("CREATE_USER_ES_USERNAME", "testuser");
    System.setProperty("CREATE_USER_ES_IAM_ROLE_ARN", ""); // Empty IAM role ARN
    Mockito.when(index.getFinalPrefix()).thenReturn("test_");

    // Act
    Function<UpgradeContext, UpgradeStepResult> executable = step.executable();
    UpgradeStepResult result = executable.apply(upgradeContext);

    // Assert
    Assert.assertNotNull(result);
    Assert.assertEquals(result.stepId(), "CreateElasticsearchUserStep");
    Assert.assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }
}
