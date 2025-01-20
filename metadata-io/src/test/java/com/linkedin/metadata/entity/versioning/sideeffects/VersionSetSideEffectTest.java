package com.linkedin.metadata.entity.versioning.sideeffects;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import io.datahubproject.metadata.context.RetrieverContext;
import jakarta.json.JsonObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class VersionSetSideEffectTest {
  private static final TestEntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final Urn TEST_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(123456,dataset)");
  private static final Urn PREVIOUS_LATEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
  private static final Urn NEW_LATEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDatasetV2,PROD)");

  private static final AspectPluginConfig TEST_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className(VersionSetSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(
              List.of("CREATE", "PATCH", "CREATE_ENTITY", "UPSERT", "DELETE", "RESTATE"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .aspectName(VERSION_SET_PROPERTIES_ASPECT_NAME)
                      .entityName(VERSION_SET_ENTITY_NAME)
                      .build()))
          .build();

  private MockAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;
  private VersionSetSideEffect sideEffect;

  @BeforeMethod
  public void setup() {
    GraphRetriever graphRetriever = mock(GraphRetriever.class);
    VersionProperties existingProperties =
        new VersionProperties()
            .setVersionSet(TEST_VERSION_SET_URN)
            .setIsLatest(false)
            .setSortId("AAAAAAAA");
    VersionProperties previousLatestProperties =
        new VersionProperties()
            .setVersionSet(TEST_VERSION_SET_URN)
            .setIsLatest(true)
            .setSortId("AAAAAAAB");
    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    data.put(NEW_LATEST_URN, Collections.singletonList(existingProperties));
    data.put(PREVIOUS_LATEST_URN, Collections.singletonList(previousLatestProperties));
    mockAspectRetriever = new MockAspectRetriever(data);
    mockAspectRetriever.setEntityRegistry(TEST_REGISTRY);

    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .aspectRetriever(mockAspectRetriever)
            .graphRetriever(graphRetriever)
            .build();

    sideEffect = new VersionSetSideEffect();
    sideEffect.setConfig(TEST_PLUGIN_CONFIG);
  }

  @Test
  public void testUpdateLatestVersion() {
    // Create previous version set properties with different latest
    VersionSetProperties previousProperties = new VersionSetProperties();
    previousProperties.setLatest(PREVIOUS_LATEST_URN);
    previousProperties.setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    // Create new version set properties
    VersionSetProperties newProperties = new VersionSetProperties();
    newProperties.setLatest(NEW_LATEST_URN);
    newProperties.setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(VERSION_SET_ENTITY_NAME);

    // Create change item
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(TEST_VERSION_SET_URN)
            .aspectName(VERSION_SET_PROPERTIES_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(VERSION_SET_PROPERTIES_ASPECT_NAME))
            .recordTemplate(newProperties)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    // Create MCL item with previous aspect
    MCLItemImpl mclItem =
        MCLItemImpl.builder()
            .previousRecordTemplate(previousProperties)
            .build(changeItem, previousProperties, null, retrieverContext.getAspectRetriever());

    // Run side effect
    List<MCPItem> sideEffectResults =
        sideEffect
            .postMCPSideEffect(Collections.singletonList(mclItem), retrieverContext)
            .collect(Collectors.toList());

    // Verify results
    assertEquals(sideEffectResults.size(), 2, "Expected two patch operations");

    // Verify patch for previous latest version
    MCPItem previousPatch = sideEffectResults.get(0);
    assertEquals(previousPatch.getUrn(), PREVIOUS_LATEST_URN);
    JsonObject previousPatchOp =
        ((PatchItemImpl) previousPatch).getPatch().toJsonArray().getJsonObject(0);
    assertEquals(previousPatchOp.getString("op"), "add");
    assertEquals(previousPatchOp.getString("path"), "/isLatest");
    assertEquals(previousPatchOp.getBoolean("value"), false);

    // Verify patch for new latest version
    MCPItem newPatch = sideEffectResults.get(1);
    assertEquals(newPatch.getUrn(), NEW_LATEST_URN);
    JsonObject newPatchOp = ((PatchItemImpl) newPatch).getPatch().toJsonArray().getJsonObject(0);
    assertEquals(newPatchOp.getString("op"), "add");
    assertEquals(newPatchOp.getString("path"), "/isLatest");
    assertEquals(newPatchOp.getBoolean("value"), true);
  }

  @Test
  public void testNoChangesWhenLatestRemainsSame() {
    // Create version set properties with same latest
    VersionSetProperties previousProperties = new VersionSetProperties();
    previousProperties.setLatest(NEW_LATEST_URN);
    previousProperties.setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    VersionSetProperties newProperties = new VersionSetProperties();
    newProperties.setLatest(NEW_LATEST_URN);
    newProperties.setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(VERSION_SET_ENTITY_NAME);

    // Create change item
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(TEST_VERSION_SET_URN)
            .aspectName(VERSION_SET_PROPERTIES_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(VERSION_SET_PROPERTIES_ASPECT_NAME))
            .recordTemplate(newProperties)
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    // Create MCL item with previous aspect
    MCLItemImpl mclItem =
        MCLItemImpl.builder()
            .previousRecordTemplate(previousProperties)
            .build(changeItem, null, null, retrieverContext.getAspectRetriever());

    // Run side effect
    List<MCPItem> sideEffectResults =
        sideEffect
            .postMCPSideEffect(Collections.singletonList(mclItem), retrieverContext)
            .collect(Collectors.toList());

    // Verify results - should still get one patch to set isLatest=true on current latest
    assertEquals(sideEffectResults.size(), 1, "Expected one patch operation");

    // Verify patch operation
    MCPItem patch = sideEffectResults.get(0);
    assertEquals(patch.getUrn(), NEW_LATEST_URN);
    JsonObject patchOp = ((PatchItemImpl) patch).getPatch().toJsonArray().getJsonObject(0);
    assertEquals(patchOp.getString("op"), "add");
    assertEquals(patchOp.getString("path"), "/isLatest");
    assertEquals(patchOp.getBoolean("value"), true);
  }

  @Test
  public void testNoChangesForNonVersionSetProperties() {
    // Create some other type of aspect change
    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(PREVIOUS_LATEST_URN)
            .aspectName(GLOBAL_TAGS_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(GLOBAL_TAGS_ASPECT_NAME))
            .recordTemplate(new GlobalTags().setTags(new TagAssociationArray()))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    MCLItemImpl mclItem =
        MCLItemImpl.builder().build(changeItem, null, null, retrieverContext.getAspectRetriever());

    // Run side effect
    List<MCPItem> sideEffectResults =
        sideEffect
            .postMCPSideEffect(Collections.singletonList(mclItem), retrieverContext)
            .collect(Collectors.toList());

    // Verify no changes for non-version set properties aspects
    assertEquals(
        sideEffectResults.size(), 0, "Expected no changes for non-version set properties aspect");
  }
}
