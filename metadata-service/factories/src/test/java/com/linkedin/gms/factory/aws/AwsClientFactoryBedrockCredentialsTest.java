package com.linkedin.gms.factory.aws;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.ObjectStorageConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AwsClientFactoryBedrockCredentialsTest {

  @Mock private ConfigurationProvider configurationProvider;

  private AwsClientFactory awsClientFactory;
  private AutoCloseable mocks;

  @BeforeMethod
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    awsClientFactory = new AwsClientFactory();
    ReflectionTestUtils.setField(awsClientFactory, "configurationProvider", configurationProvider);
    System.clearProperty("AWS_REGION");
    System.clearProperty("AWS_ENDPOINT_URL");
    System.clearProperty("aws.region");

    DataHubConfiguration dataHubConfiguration = new DataHubConfiguration();
    dataHubConfiguration.setObjectStorage(new ObjectStorageConfiguration());
    when(configurationProvider.getDatahub()).thenReturn(dataHubConfiguration);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    System.clearProperty("AWS_REGION");
    System.clearProperty("AWS_ENDPOINT_URL");
    System.clearProperty("aws.region");
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void bedrockEmbeddingConfigRequiresSharedCredentialsEvenWithoutPodRegion() {
    wireBedrockConfig("us-west-2");
    assertTrue(awsClientFactory.isBedrockEmbeddingConfigured());
    assertTrue(awsClientFactory.isAwsCredentialsRequired());
  }

  @Test
  public void nonBedrockSemanticSearchDoesNotRequireBedrockCredentials() {
    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    EmbeddingProviderConfiguration embeddingProvider = new EmbeddingProviderConfiguration();
    embeddingProvider.setType("openai");
    semanticSearch.setEmbeddingProvider(embeddingProvider);

    EntityIndexConfiguration entityIndex = new EntityIndexConfiguration();
    entityIndex.setSemanticSearch(semanticSearch);
    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setEntityIndex(entityIndex);
    when(configurationProvider.getElasticSearch()).thenReturn(esConfig);

    assertFalse(awsClientFactory.isBedrockEmbeddingConfigured());
  }

  @Test
  public void openSearchIamAuthRequiresSharedCredentialsEvenWithoutPodRegion() {
    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setOpensearchUseAwsIamAuth(true);
    esConfig.setRegion("us-east-1");
    when(configurationProvider.getElasticSearch()).thenReturn(esConfig);

    assertTrue(awsClientFactory.isOpenSearchIamAuthConfigured());
    assertTrue(awsClientFactory.isAwsCredentialsRequired());
  }

  @Test
  public void objectStorageRoleArnRequiresSharedCredentialsEvenWithoutPodRegion() {
    DataHubConfiguration dataHubConfiguration = new DataHubConfiguration();
    ObjectStorageConfiguration objectStorage = new ObjectStorageConfiguration();
    objectStorage.setRoleArn("arn:aws:iam::123456789012:role/test-role");
    dataHubConfiguration.setObjectStorage(objectStorage);
    when(configurationProvider.getDatahub()).thenReturn(dataHubConfiguration);
    when(configurationProvider.getElasticSearch()).thenReturn(new ElasticSearchConfiguration());

    assertTrue(awsClientFactory.isObjectStorageRoleArnConfigured());
    assertTrue(awsClientFactory.isAwsCredentialsRequired());
  }

  private void wireBedrockConfig(String bedrockRegion) {
    EmbeddingProviderConfiguration.BedrockConfig bedrock =
        new EmbeddingProviderConfiguration.BedrockConfig();
    bedrock.setAwsRegion(bedrockRegion);

    EmbeddingProviderConfiguration embeddingProvider = new EmbeddingProviderConfiguration();
    embeddingProvider.setType("aws-bedrock");
    embeddingProvider.setBedrock(bedrock);

    SemanticSearchConfiguration semanticSearch = new SemanticSearchConfiguration();
    semanticSearch.setEnabled(true);
    semanticSearch.setEmbeddingProvider(embeddingProvider);

    EntityIndexConfiguration entityIndex = new EntityIndexConfiguration();
    entityIndex.setSemanticSearch(semanticSearch);

    ElasticSearchConfiguration esConfig = new ElasticSearchConfiguration();
    esConfig.setEntityIndex(entityIndex);

    when(configurationProvider.getElasticSearch()).thenReturn(esConfig);
  }
}
