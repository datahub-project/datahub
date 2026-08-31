package com.linkedin.datahub.graphql.resolvers.settings.asset;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AssetSettings;
import com.linkedin.datahub.graphql.generated.UpdateAssetSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateAssetSummaryInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.asset.AssetSummarySettings;
import com.linkedin.settings.asset.AssetSummarySettingsTemplate;
import com.linkedin.settings.asset.AssetSummarySettingsTemplateArray;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateAssetSettingsResolverTest {

  private static final String TEST_ASSET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)";
  private static final String TEST_TEMPLATE_URN = "urn:li:dataHubPageTemplate:testTemplate";

  private EntityClient mockEntityClient;
  private UpdateAssetSettingsResolver resolver;
  private QueryContext mockContext;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
  public void setUp() {
    mockEntityClient = mock(EntityClient.class);
    resolver = new UpdateAssetSettingsResolver(mockEntityClient);
    mockContext = getMockAllowContext();
    mockEnv = mock(DataFetchingEnvironment.class);
    when(mockEnv.getContext()).thenReturn(mockContext);
  }

  @Test
  public void testConstructorWithNullEntityClient() {
    assertThrows(NullPointerException.class, () -> new UpdateAssetSettingsResolver(null));
  }

  @Test
  public void testUpdateAssetSettingsWithNoExistingSettings() throws Exception {
    // Arrange
    UpdateAssetSettingsInput input = createTestInput();
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Mock no existing aspect
    when(mockEntityClient.getLatestAspectObject(
            any(), any(Urn.class), eq(Constants.ASSET_SETTINGS_ASPECT_NAME), eq(false)))
        .thenReturn(null);

    // Act
    CompletableFuture<AssetSettings> result = resolver.get(mockEnv);
    AssetSettings assetSettings = result.get();

    // Assert
    assertNotNull(assetSettings);
    assertNotNull(assetSettings.getAssetSummary());
    assertNotNull(assetSettings.getAssetSummary().getTemplates());
    assertEquals(assetSettings.getAssetSummary().getTemplates().size(), 1);
    assertEquals(
        assetSettings.getAssetSummary().getTemplates().get(0).getTemplate().getUrn(),
        TEST_TEMPLATE_URN);

    // Verify entity client interactions
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(TEST_ASSET_URN)),
            eq(Constants.ASSET_SETTINGS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testUpdateAssetSettingsWithExistingSettings() throws Exception {
    // Arrange
    UpdateAssetSettingsInput input = createTestInput();
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Mock existing aspect
    com.linkedin.settings.asset.AssetSettings existingSettings =
        new com.linkedin.settings.asset.AssetSettings();
    AssetSummarySettings existingAssetSummary = new AssetSummarySettings();
    AssetSummarySettingsTemplateArray existingTemplates = new AssetSummarySettingsTemplateArray();
    AssetSummarySettingsTemplate existingTemplate = new AssetSummarySettingsTemplate();
    existingTemplate.setTemplate(UrnUtils.getUrn("urn:li:dataHubPageTemplate:oldTemplate"));
    existingTemplates.add(existingTemplate);
    existingAssetSummary.setTemplates(existingTemplates);
    existingSettings.setAssetSummary(existingAssetSummary);

    Aspect mockAspect = mock(Aspect.class);
    when(mockAspect.data()).thenReturn(existingSettings.data());
    when(mockEntityClient.getLatestAspectObject(
            any(), any(Urn.class), eq(Constants.ASSET_SETTINGS_ASPECT_NAME), eq(false)))
        .thenReturn(mockAspect);

    // Act
    CompletableFuture<AssetSettings> result = resolver.get(mockEnv);
    AssetSettings assetSettings = result.get();

    // Assert
    assertNotNull(assetSettings);
    assertNotNull(assetSettings.getAssetSummary());
    assertNotNull(assetSettings.getAssetSummary().getTemplates());
    assertEquals(assetSettings.getAssetSummary().getTemplates().size(), 1);
    // Should have the new template, not the old one
    assertEquals(
        assetSettings.getAssetSummary().getTemplates().get(0).getTemplate().getUrn(),
        TEST_TEMPLATE_URN);

    // Verify entity client interactions
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(TEST_ASSET_URN)),
            eq(Constants.ASSET_SETTINGS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testUpdateAssetSettingsWithNullSummaryInput() throws Exception {
    // Arrange
    UpdateAssetSettingsInput input = new UpdateAssetSettingsInput();
    input.setUrn(TEST_ASSET_URN);
    input.setSummary(null); // No summary input
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Mock no existing aspect
    when(mockEntityClient.getLatestAspectObject(
            any(), any(Urn.class), eq(Constants.ASSET_SETTINGS_ASPECT_NAME), eq(false)))
        .thenReturn(null);

    // Act
    CompletableFuture<AssetSettings> result = resolver.get(mockEnv);
    AssetSettings assetSettings = result.get();

    // Assert
    assertNotNull(assetSettings);
    // Asset summary should be null since no summary input was provided
    assertNull(assetSettings.getAssetSummary());

    // Verify entity client interactions
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(TEST_ASSET_URN)),
            eq(Constants.ASSET_SETTINGS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testUpdateAssetSettingsWithNullTemplateInSummary() throws Exception {
    // Arrange
    UpdateAssetSettingsInput input = new UpdateAssetSettingsInput();
    input.setUrn(TEST_ASSET_URN);
    UpdateAssetSummaryInput summaryInput = new UpdateAssetSummaryInput();
    summaryInput.setTemplate(null); // Null template
    input.setSummary(summaryInput);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Mock no existing aspect
    when(mockEntityClient.getLatestAspectObject(
            any(), any(Urn.class), eq(Constants.ASSET_SETTINGS_ASPECT_NAME), eq(false)))
        .thenReturn(null);

    // Act
    CompletableFuture<AssetSettings> result = resolver.get(mockEnv);
    AssetSettings assetSettings = result.get();

    // Assert
    assertNotNull(assetSettings);
    // Asset summary should be null since template was null
    assertNull(assetSettings.getAssetSummary());

    // Verify entity client interactions
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(TEST_ASSET_URN)),
            eq(Constants.ASSET_SETTINGS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testGetLatestAspectObjectException() throws Exception {
    // Arrange
    UpdateAssetSettingsInput input = createTestInput();
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Mock exception from entity client
    when(mockEntityClient.getLatestAspectObject(
            any(), any(Urn.class), eq(Constants.ASSET_SETTINGS_ASPECT_NAME), eq(false)))
        .thenThrow(new RuntimeException("Failed to get aspect"));

    // Act & Assert
    CompletableFuture<AssetSettings> result = resolver.get(mockEnv);
    assertThrows(CompletionException.class, () -> result.join());

    // Verify entity client interactions
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(TEST_ASSET_URN)),
            eq(Constants.ASSET_SETTINGS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testIngestProposalException() throws Exception {
    // Arrange
    UpdateAssetSettingsInput input = createTestInput();
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Mock successful aspect retrieval
    when(mockEntityClient.getLatestAspectObject(
            any(), any(Urn.class), eq(Constants.ASSET_SETTINGS_ASPECT_NAME), eq(false)))
        .thenReturn(null);

    // Mock exception from ingest proposal
    doThrow(new RuntimeException("Failed to ingest proposal"))
        .when(mockEntityClient)
        .ingestProposal(any(), any(MetadataChangeProposal.class), eq(false));

    // Act & Assert
    CompletableFuture<AssetSettings> result = resolver.get(mockEnv);
    assertThrows(CompletionException.class, () -> result.join());

    // Verify entity client interactions
    verify(mockEntityClient, times(1))
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(TEST_ASSET_URN)),
            eq(Constants.ASSET_SETTINGS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testInvalidAssetUrn() throws Exception {
    // Arrange
    UpdateAssetSettingsInput input = new UpdateAssetSettingsInput();
    input.setUrn("invalid-urn");
    input.setSummary(createTestSummaryInput());
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    // Act & Assert
    // The UrnUtils.getUrn() call happens synchronously BEFORE the CompletableFuture
    // so it throws RuntimeException directly
    assertThrows(RuntimeException.class, () -> resolver.get(mockEnv));

    // Verify no entity client interactions due to invalid URN
    verify(mockEntityClient, never())
        .getLatestAspectObject(any(), any(Urn.class), anyString(), anyBoolean());
    verify(mockEntityClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testMultipleSuccessfulUpdates() throws Exception {
    // Test that multiple calls work correctly
    UpdateAssetSettingsInput input1 = createTestInput();
    UpdateAssetSettingsInput input2 = createTestInputWithDifferentTemplate();

    when(mockEntityClient.getLatestAspectObject(
            any(), any(Urn.class), eq(Constants.ASSET_SETTINGS_ASPECT_NAME), eq(false)))
        .thenReturn(null);

    // First update
    when(mockEnv.getArgument(eq("input"))).thenReturn(input1);
    CompletableFuture<AssetSettings> result1 = resolver.get(mockEnv);
    AssetSettings assetSettings1 = result1.get();

    // Second update
    when(mockEnv.getArgument(eq("input"))).thenReturn(input2);
    CompletableFuture<AssetSettings> result2 = resolver.get(mockEnv);
    AssetSettings assetSettings2 = result2.get();

    // Assert both calls succeeded
    assertNotNull(assetSettings1);
    assertNotNull(assetSettings2);
    assertEquals(
        assetSettings1.getAssetSummary().getTemplates().get(0).getTemplate().getUrn(),
        TEST_TEMPLATE_URN);
    assertEquals(
        assetSettings2.getAssetSummary().getTemplates().get(0).getTemplate().getUrn(),
        "urn:li:dataHubPageTemplate:differentTemplate");

    // Verify entity client interactions
    verify(mockEntityClient, times(2))
        .getLatestAspectObject(
            any(),
            eq(UrnUtils.getUrn(TEST_ASSET_URN)),
            eq(Constants.ASSET_SETTINGS_ASPECT_NAME),
            eq(false));
    verify(mockEntityClient, times(2))
        .ingestProposal(any(), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testDataFetchingEnvironmentException() throws Exception {
    // Arrange
    when(mockEnv.getArgument(eq("input"))).thenThrow(new RuntimeException("Environment error"));

    // Act & Assert
    assertThrows(Exception.class, () -> resolver.get(mockEnv));

    // Verify no entity client interactions due to environment error
    verify(mockEntityClient, never())
        .getLatestAspectObject(any(), any(Urn.class), anyString(), anyBoolean());
    verify(mockEntityClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  private UpdateAssetSettingsInput createTestInput() {
    UpdateAssetSettingsInput input = new UpdateAssetSettingsInput();
    input.setUrn(TEST_ASSET_URN);
    input.setSummary(createTestSummaryInput());
    return input;
  }

  private UpdateAssetSummaryInput createTestSummaryInput() {
    UpdateAssetSummaryInput summaryInput = new UpdateAssetSummaryInput();
    summaryInput.setTemplate(TEST_TEMPLATE_URN);
    return summaryInput;
  }

  private UpdateAssetSettingsInput createTestInputWithDifferentTemplate() {
    UpdateAssetSettingsInput input = new UpdateAssetSettingsInput();
    input.setUrn(TEST_ASSET_URN);
    UpdateAssetSummaryInput summaryInput = new UpdateAssetSummaryInput();
    summaryInput.setTemplate("urn:li:dataHubPageTemplate:differentTemplate");
    input.setSummary(summaryInput);
    return input;
  }
}
