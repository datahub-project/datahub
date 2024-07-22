package com.linkedin.metadata.aspect.hooks;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.config.structuredProperties.extensions.ExtendedModelValidationConfiguration;
import com.linkedin.metadata.config.structuredProperties.extensions.ModelExtensionValidationConfiguration;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ExtendedModelStructuredPropertyMutatorTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(IgnoreUnknownMutator.class.getName())
          .enabled(true)
          .supportedOperations(List.of("UPSERT"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(DATASET_ENTITY_NAME)
                      .aspectName("*")
                      .build()))
          .build();
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:postgres,calm-pagoda-323403.jaffle_shop.customers,PROD)");
  private static final Urn TEST_CORP_USER_URN =
      UrnUtils.getUrn(
          "urn:li:corpuser:someName");
  private AspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;
  private EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .aspectRetriever(mockAspectRetriever)
            .graphRetriever(TestOperationContexts.emptyGraphRetriever)
            .build();
  }

  @Test
  public void testUnknownFieldInTagAssociationArray() throws Exception {
    ModelExtensionValidationConfiguration modelExtensionValidationConfiguration =
        new ModelExtensionValidationConfiguration();
    modelExtensionValidationConfiguration.setEnabled(true);
    modelExtensionValidationConfiguration.setConfigFile("extended_properties_config_test.yml");
    ExtendedModelValidationConfiguration extendedModelValidationConfiguration =
        modelExtensionValidationConfiguration.resolve(new YAMLMapper());
    ExtendedModelStructuredPropertyMutator test =
        new ExtendedModelStructuredPropertyMutator(extendedModelValidationConfiguration, true);
    test.setConfig(TEST_PLUGIN_CONFIG);

    List<MCPItem> testItems =
        List.of(
            ProposedItem.builder()
                .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
                .metadataChangeProposal(
                    new MetadataChangeProposal()
                        .setEntityUrn(TEST_DATASET_URN)
                        .setAspectName(GLOBAL_TAGS_ASPECT_NAME)
                        .setEntityType(DATASET_ENTITY_NAME)
                        .setChangeType(ChangeType.UPSERT)
                        .setAspect(
                            new GenericAspect()
                                .setContentType("application/json")
                                .setValue(
                                    ByteString.copyString(
                                        "{\"tags\":[{\"tag\":\"urn:li:tag:Legacy\",\"foo\":\"bar\"}]}",
                                        StandardCharsets.UTF_8)))
                        .setSystemMetadata(new SystemMetadata()))
                .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                .build());

    List<MCPItem> result = test.proposalMutation(testItems, retrieverContext).toList();

    assertEquals(1, result.size());
    assertEquals(
        result.get(0).getAspect(GlobalTags.class),
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    List.of(
                        new TagAssociation()
                            .setTag(TagUrn.createFromString("urn:li:tag:Legacy"))))));
  }

  @Test
  public void testUnknownFieldDatasetProperties() throws Exception {
    ModelExtensionValidationConfiguration modelExtensionValidationConfiguration =
        new ModelExtensionValidationConfiguration();
    modelExtensionValidationConfiguration.setEnabled(true);
    modelExtensionValidationConfiguration.setConfigFile("extended_properties_config_test.yml");
    ExtendedModelValidationConfiguration extendedModelValidationConfiguration =
        modelExtensionValidationConfiguration.resolve(new YAMLMapper());
    ExtendedModelStructuredPropertyMutator test =
        new ExtendedModelStructuredPropertyMutator(extendedModelValidationConfiguration, true);
    test.setConfig(TEST_PLUGIN_CONFIG);

    List<MCPItem> testItems =
        List.of(
            ProposedItem.builder()
                .entitySpec(TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME))
                .metadataChangeProposal(
                    new MetadataChangeProposal()
                        .setEntityUrn(TEST_DATASET_URN)
                        .setAspectName(DATASET_PROPERTIES_ASPECT_NAME)
                        .setEntityType(DATASET_ENTITY_NAME)
                        .setChangeType(ChangeType.UPSERT)
                        .setAspect(
                            new GenericAspect()
                                .setContentType("application/json")
                                .setValue(
                                    ByteString.copyString(
                                        "{\"foo\":\"bar\",\"customProperties\":{\"prop2\":\"pikachu\",\"prop1\":\"fakeprop\"}}",
                                        StandardCharsets.UTF_8)))
                        .setSystemMetadata(new SystemMetadata()))
                .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                .build());

    List<MCPItem> result = test.proposalMutation(testItems, retrieverContext).toList();

    assertEquals(1, result.size());
    assertEquals(
        result.get(0).getAspect(DatasetProperties.class),
        new DatasetProperties()
            .setCustomProperties(new StringMap(Map.of("prop1", "fakeprop", "prop2", "pikachu"))));
  }

  @Test
  public void testUnknownFieldMappedToStructuredProp() throws Exception {
    ModelExtensionValidationConfiguration modelExtensionValidationConfiguration =
        new ModelExtensionValidationConfiguration();
    modelExtensionValidationConfiguration.setEnabled(true);
    modelExtensionValidationConfiguration.setConfigFile("extended_properties_config_test.yml");
    ExtendedModelValidationConfiguration extendedModelValidationConfiguration =
        modelExtensionValidationConfiguration.resolve(new YAMLMapper());
    ExtendedModelStructuredPropertyMutator test =
        new ExtendedModelStructuredPropertyMutator(extendedModelValidationConfiguration, true);
    test.setConfig(TEST_PLUGIN_CONFIG);

    List<MCPItem> testItems =
        List.of(
            ProposedItem.builder()
                .entitySpec(TEST_REGISTRY.getEntitySpec(CORP_USER_ENTITY_NAME))
                .metadataChangeProposal(
                    new MetadataChangeProposal()
                        .setEntityUrn(TEST_CORP_USER_URN)
                        .setAspectName(CORP_USER_INFO_ASPECT_NAME)
                        .setEntityType(CORP_USER_ENTITY_NAME)
                        .setChangeType(ChangeType.UPSERT)
                        .setAspect(
                            new GenericAspect()
                                .setContentType("application/json")
                                .setValue(
                                    ByteString.copyString(
                                        "{\"internalId\":\"1234\",\"active\":true}",
                                        StandardCharsets.UTF_8)))
                        .setSystemMetadata(new SystemMetadata()))
                .auditStamp(AuditStampUtils.createDefaultAuditStamp())
                .build());

    List<MCPItem> result = test.proposalMutation(testItems, retrieverContext).toList();

    assertEquals(2, result.size());

    assertEquals(
        result.get(0).getAspect(CorpUserInfo.class),
        new CorpUserInfo()
            .setActive(true));
    assertEquals(result.get(1).getMetadataChangeProposal().getAspectName(), STRUCTURED_PROPERTIES_ASPECT_NAME);
  }

  @Test
  public void testConfigResolution() throws Exception {
    ModelExtensionValidationConfiguration modelExtensionValidationConfiguration =
        new ModelExtensionValidationConfiguration();
    modelExtensionValidationConfiguration.setEnabled(true);
    modelExtensionValidationConfiguration.setConfigFile("extended_properties_config_test.yml");
    ExtendedModelValidationConfiguration extendedModelValidationConfiguration =
        modelExtensionValidationConfiguration.resolve(new YAMLMapper());

    assertEquals(extendedModelValidationConfiguration.getEntities().get(0).getEntity(), "dataset");
  }
}
