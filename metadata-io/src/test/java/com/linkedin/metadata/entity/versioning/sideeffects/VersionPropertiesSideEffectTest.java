package com.linkedin.metadata.entity.versioning.sideeffects;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder.*;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.VersionTag;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.ebean.batch.MCLItemImpl;
import com.linkedin.metadata.key.VersionSetKey;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class VersionPropertiesSideEffectTest {
  private static final TestEntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  private static final Urn HAS_PROPERTIES_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(has-properties,dataset)");
  private static final Urn MISSING_PROPERTIES_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(missing-properties-exists,dataset)");
  private static final Urn NOT_EXISTS_PROPERTIES_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(does-not-exist,dataset)");

  private static final Urn PREVIOUS_LATEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
  private static final Urn ENTITY_URN =
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
                      .entityName(ALL)
                      .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
                      .build()))
          .build();

  private MockAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;
  private VersionPropertiesSideEffect sideEffect;

  @BeforeMethod
  public void setup() {
    VersionSetProperties existingVersionSetProperties =
        new VersionSetProperties()
            .setLatest(PREVIOUS_LATEST_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING);
    VersionSetKey existingVersionSetKey =
        new VersionSetKey().setId("missing-properties-exists").setEntityType(DATASET_ENTITY_NAME);

    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    data.put(HAS_PROPERTIES_VERSION_SET_URN, List.of(existingVersionSetProperties));
    data.put(MISSING_PROPERTIES_VERSION_SET_URN, List.of(existingVersionSetKey));
    mockAspectRetriever = new MockAspectRetriever(data);
    mockAspectRetriever.setEntityRegistry(TEST_REGISTRY);

    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .aspectRetriever(mockAspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();

    sideEffect = new VersionPropertiesSideEffect();
    sideEffect.setConfig(TEST_PLUGIN_CONFIG);
  }

  @Test
  public void testCreateVersionSet() {
    // Create version set if it does not exist
    VersionProperties properties =
        new VersionProperties()
            .setVersionSet(NOT_EXISTS_PROPERTIES_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB)
            .setVersion(new VersionTag().setVersionTag("version"))
            .setSortId("abc");

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_URN)
            .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(VERSION_PROPERTIES_ASPECT_NAME))
            .recordTemplate(properties)
            .previousSystemAspect(mock(SystemAspect.class))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    // Run side effect
    List<MCPItem> sideEffectResults =
        sideEffect
            .applyMCPSideEffect(Collections.singletonList(changeItem), retrieverContext)
            .collect(Collectors.toList());

    // Verify results
    assertEquals(sideEffectResults.size(), 1, "Expected one upsert");

    // Verify patch for previous latest version
    MCPItem patched = sideEffectResults.get(0);
    assertEquals(patched.getUrn(), NOT_EXISTS_PROPERTIES_VERSION_SET_URN);
    VersionSetProperties versionSetProperties = patched.getAspect(VersionSetProperties.class);
    assertNotNull(versionSetProperties);
    assertEquals(versionSetProperties.getLatest(), ENTITY_URN);
    assertEquals(
        versionSetProperties.getVersioningScheme(),
        VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);
  }

  @Test
  public void testVersionSetPropertiesExists() {
    // Do nothing if version set properties already exists
    VersionProperties properties =
        new VersionProperties()
            .setVersionSet(HAS_PROPERTIES_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("version"))
            .setSortId("abc");

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_URN)
            .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(VERSION_PROPERTIES_ASPECT_NAME))
            .recordTemplate(properties)
            .previousSystemAspect(mock(SystemAspect.class))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    // Run side effect
    List<MCPItem> sideEffectResults =
        sideEffect
            .applyMCPSideEffect(Collections.singletonList(changeItem), retrieverContext)
            .collect(Collectors.toList());

    // Verify results
    assertEquals(sideEffectResults.size(), 0, "Expected no operations");
  }

  @Test
  public void testCreateVersionSetExists() {
    // Create version set properties if entity exists but properties aspect does not
    VersionProperties properties =
        new VersionProperties()
            .setVersionSet(MISSING_PROPERTIES_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("version"))
            .setSortId("abc");

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ENTITY_URN)
            .aspectName(VERSION_PROPERTIES_ASPECT_NAME)
            .entitySpec(entitySpec)
            .aspectSpec(entitySpec.getAspectSpec(VERSION_PROPERTIES_ASPECT_NAME))
            .recordTemplate(properties)
            .previousSystemAspect(mock(SystemAspect.class))
            .auditStamp(AuditStampUtils.createDefaultAuditStamp())
            .build(mockAspectRetriever);

    // Run side effect
    List<MCPItem> sideEffectResults =
        sideEffect
            .applyMCPSideEffect(Collections.singletonList(changeItem), retrieverContext)
            .collect(Collectors.toList());

    // Verify results
    assertEquals(sideEffectResults.size(), 1, "Expected one upsert");

    // Verify patch for previous latest version
    MCPItem patched = sideEffectResults.get(0);
    assertEquals(patched.getUrn(), MISSING_PROPERTIES_VERSION_SET_URN);
    VersionSetProperties versionSetProperties = patched.getAspect(VersionSetProperties.class);
    assertNotNull(versionSetProperties);
    assertEquals(versionSetProperties.getLatest(), ENTITY_URN);
    assertEquals(versionSetProperties.getVersioningScheme(), VersioningScheme.LEXICOGRAPHIC_STRING);
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
