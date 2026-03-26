package com.linkedin.metadata.entity.versioning.sideeffects;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.*;
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
import java.util.Optional;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class VersionPropertiesSideEffectTest {
  private static final TestEntityRegistry TEST_REGISTRY = new TestEntityRegistry();

  // Standard version set with a single entity in it, `PREVIOUS_LATEST_URN`
  private static final Urn HAS_SET_PROPERTIES_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(has-properties,dataset)");

  // Version set urn with a VersionSetKey but no VersionSetProperties
  private static final Urn MISSING_SET_PROPERTIES_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(missing-properties,mlModel)");

  // Version set urn that does not exist
  private static final Urn NON_EXISTENT_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(does-not-exist,dataset)");

  // Its latest urn does not have a version properties aspect
  private static final Urn INVALID_VERSION_SET_URN =
      UrnUtils.getUrn("urn:li:versionSet:(invalid-properties,dataset)");

  private static final Urn PREVIOUS_LATEST_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,previous,PROD)");
  private static final Urn MISSING_VERSION_PROPERTIES_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,invalid,PROD)");
  private static final Urn ENTITY_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,entity,PROD)");
  private static final Urn ML_MODEL_URN =
      UrnUtils.getUrn("urn:li:mlModel:(urn:li:dataPlatform:mlflow,model,PROD)");

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
    VersionProperties previousLatestVersionProperties =
        new VersionProperties()
            .setVersionSet(HAS_SET_PROPERTIES_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v1"))
            .setSortId("abc");
    VersionSetProperties existingVersionSetProperties =
        new VersionSetProperties()
            .setLatest(PREVIOUS_LATEST_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING);
    VersionSetProperties invalidVersionSetProperties =
        new VersionSetProperties()
            .setLatest(MISSING_VERSION_PROPERTIES_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING);
    VersionSetKey existingVersionSetKey =
        new VersionSetKey().setId("missing-properties-exists").setEntityType(DATASET_ENTITY_NAME);

    Map<Urn, List<RecordTemplate>> data = new HashMap<>();
    data.put(PREVIOUS_LATEST_URN, List.of(previousLatestVersionProperties));
    data.put(HAS_SET_PROPERTIES_VERSION_SET_URN, List.of(existingVersionSetProperties));
    data.put(MISSING_SET_PROPERTIES_VERSION_SET_URN, List.of(existingVersionSetKey));
    data.put(INVALID_VERSION_SET_URN, List.of(invalidVersionSetProperties));
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
            .setVersionSet(NON_EXISTENT_VERSION_SET_URN)
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

    // Verify results: key, set properties, and isLatest=true write for the entity
    assertEquals(sideEffectResults.size(), 3, "Expected three mcps: key, set properties, isLatest");

    MCPItem keyMCP = sideEffectResults.get(0);
    assertEquals(keyMCP.getUrn(), NON_EXISTENT_VERSION_SET_URN);
    VersionSetKey versionSetKey = keyMCP.getAspect(VersionSetKey.class);
    assertNotNull(versionSetKey);
    assertEquals(versionSetKey.getId(), "does-not-exist");
    assertEquals(versionSetKey.getEntityType(), DATASET_ENTITY_NAME);

    MCPItem setPropertiesMCP = sideEffectResults.get(1);
    assertEquals(setPropertiesMCP.getUrn(), NON_EXISTENT_VERSION_SET_URN);
    VersionSetProperties versionSetProperties =
        setPropertiesMCP.getAspect(VersionSetProperties.class);
    assertNotNull(versionSetProperties);
    assertEquals(versionSetProperties.getLatest(), ENTITY_URN);
    assertEquals(versionSetProperties.getVersioningScheme(), VersioningScheme.LEXICOGRAPHIC_STRING);

    // isLatest=true is written as a separate ChangeItemImpl targeting the entity, not via
    // in-place mutation of the incoming recordTemplate.
    MCPItem setIsLatestMCP = sideEffectResults.get(2);
    assertEquals(setIsLatestMCP.getUrn(), ENTITY_URN);
    VersionProperties entityVersionProperties = setIsLatestMCP.getAspect(VersionProperties.class);
    assertNotNull(entityVersionProperties);
    assertTrue(entityVersionProperties.isIsLatest());
  }

  @Test
  public void testUpdateLatest() {
    // Upsert version set properties with new latest; update old latest version properties
    VersionProperties properties =
        new VersionProperties()
            .setVersionSet(HAS_SET_PROPERTIES_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("version"))
            .setSortId("bbb");

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

    // Verify results: set properties, old-latest patch, and isLatest=true write for new entity
    assertEquals(
        sideEffectResults.size(),
        3,
        "Expected three mcps: set properties, old latest patch, and isLatest write");

    MCPItem setPropertiesMCP = sideEffectResults.get(0);
    assertEquals(setPropertiesMCP.getUrn(), HAS_SET_PROPERTIES_VERSION_SET_URN);
    VersionSetProperties versionSetProperties =
        setPropertiesMCP.getAspect(VersionSetProperties.class);
    assertNotNull(versionSetProperties);
    assertEquals(versionSetProperties.getLatest(), ENTITY_URN);
    assertEquals(versionSetProperties.getVersioningScheme(), VersioningScheme.LEXICOGRAPHIC_STRING);

    MCPItem oldLatestMCP = sideEffectResults.get(1);
    assertEquals(oldLatestMCP.getUrn(), PREVIOUS_LATEST_URN);
    VersionProperties oldLatestVersionProperties = oldLatestMCP.getAspect(VersionProperties.class);
    assertNotNull(oldLatestVersionProperties);
    assertFalse(oldLatestVersionProperties.isIsLatest());

    MCPItem setIsLatestMCP = sideEffectResults.get(2);
    assertEquals(setIsLatestMCP.getUrn(), ENTITY_URN);
    VersionProperties entityVersionProperties = setIsLatestMCP.getAspect(VersionProperties.class);
    assertNotNull(entityVersionProperties);
    assertTrue(entityVersionProperties.isIsLatest());
  }

  @Test
  public void testNotNewLatest() {
    // Do nothing if not changing latest
    VersionProperties properties =
        new VersionProperties()
            .setVersionSet(HAS_SET_PROPERTIES_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("version"))
            .setSortId("aaa");

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
    assertFalse(properties.hasIsLatest(), "Incoming recordTemplate must not be mutated");
    assertEquals(sideEffectResults.size(), 0, "Expected no operations");
  }

  @Test
  public void testCreateVersionSetKeyExists() {
    // Create version set properties if entity exists but properties aspect does not
    VersionProperties properties =
        new VersionProperties()
            .setVersionSet(MISSING_SET_PROPERTIES_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("version"))
            .setSortId("abc");

    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(ML_MODEL_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(ML_MODEL_URN)
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

    // Verify results: key, set properties, and isLatest=true write for the entity
    assertEquals(sideEffectResults.size(), 3, "Expected three mcps: key, set properties, isLatest");

    MCPItem keyMCP = sideEffectResults.get(0);
    assertEquals(keyMCP.getUrn(), MISSING_SET_PROPERTIES_VERSION_SET_URN);
    VersionSetKey versionSetKey = keyMCP.getAspect(VersionSetKey.class);
    assertNotNull(versionSetKey);
    assertEquals(versionSetKey.getId(), "missing-properties");
    assertEquals(versionSetKey.getEntityType(), ML_MODEL_ENTITY_NAME);

    MCPItem setPropertiesMCP = sideEffectResults.get(1);
    assertEquals(setPropertiesMCP.getUrn(), MISSING_SET_PROPERTIES_VERSION_SET_URN);
    VersionSetProperties versionSetProperties =
        setPropertiesMCP.getAspect(VersionSetProperties.class);
    assertNotNull(versionSetProperties);
    assertEquals(versionSetProperties.getLatest(), ML_MODEL_URN);
    assertEquals(versionSetProperties.getVersioningScheme(), VersioningScheme.LEXICOGRAPHIC_STRING);

    MCPItem setIsLatestMCP = sideEffectResults.get(2);
    assertEquals(setIsLatestMCP.getUrn(), ML_MODEL_URN);
    VersionProperties entityVersionProperties = setIsLatestMCP.getAspect(VersionProperties.class);
    assertNotNull(entityVersionProperties);
    assertTrue(entityVersionProperties.isIsLatest());
  }

  @Test
  public void testUpdateLatestInvalidPreviousLatest() {
    // Upsert version set properties with new latest; update old latest version properties
    VersionProperties properties =
        new VersionProperties()
            .setVersionSet(INVALID_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("version"))
            .setSortId("bbb");

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

    // Verify results: set properties and isLatest=true write for the entity
    assertEquals(sideEffectResults.size(), 2, "Expected two mcps: set properties and isLatest");

    MCPItem setPropertiesMCP = sideEffectResults.get(0);
    assertEquals(setPropertiesMCP.getUrn(), INVALID_VERSION_SET_URN);
    VersionSetProperties versionSetProperties =
        setPropertiesMCP.getAspect(VersionSetProperties.class);
    assertEquals(versionSetProperties.getLatest(), ENTITY_URN);
    assertEquals(versionSetProperties.getVersioningScheme(), VersioningScheme.LEXICOGRAPHIC_STRING);

    MCPItem setIsLatestMCP = sideEffectResults.get(1);
    assertEquals(setIsLatestMCP.getUrn(), ENTITY_URN);
    VersionProperties entityVersionProperties = setIsLatestMCP.getAspect(VersionProperties.class);
    assertNotNull(entityVersionProperties);
    assertTrue(entityVersionProperties.isIsLatest());
  }

  @Test
  public void testNoChangesForNonVersionSetProperties() {
    // Create some other type of aspect change
    EntitySpec entitySpec = TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME);
    ChangeItemImpl changeItem =
        ChangeItemImpl.builder()
            .urn(MISSING_VERSION_PROPERTIES_URN)
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

  @Test
  public void testOriginalRecordTemplateNotMutatedOnCreateVersionSet() {
    VersionProperties properties =
        new VersionProperties()
            .setVersionSet(NON_EXISTENT_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v1"))
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

    // First pass (initial transaction attempt)
    sideEffect
        .applyMCPSideEffect(Collections.singletonList(changeItem), retrieverContext)
        .collect(Collectors.toList());

    // The incoming recordTemplate must not have isLatest set — if it were mutated here,
    // validateProposedAspects on a retry would reject it with "IsLatest should not be specified".
    assertFalse(
        properties.hasIsLatest(),
        "Side effect must not mutate the incoming VersionProperties DataMap");

    // Second pass (simulated transaction retry) — must produce the same result without error
    List<MCPItem> retryResults =
        sideEffect
            .applyMCPSideEffect(Collections.singletonList(changeItem), retrieverContext)
            .collect(Collectors.toList());

    assertEquals(retryResults.size(), 3, "Retry must produce the same three MCPs");

    // isLatest=true must still be communicated via the returned ChangeItemImpl, not via mutation
    Optional<MCPItem> setIsLatestMCP =
        retryResults.stream().filter(item -> item.getUrn().equals(ENTITY_URN)).findFirst();
    assertTrue(setIsLatestMCP.isPresent());
    assertTrue(setIsLatestMCP.get().getAspect(VersionProperties.class).isIsLatest());
  }

  @Test
  public void testOriginalRecordTemplateNotMutatedOnUpdateLatest() {
    VersionProperties properties =
        new VersionProperties()
            .setVersionSet(HAS_SET_PROPERTIES_VERSION_SET_URN)
            .setVersioningScheme(VersioningScheme.LEXICOGRAPHIC_STRING)
            .setVersion(new VersionTag().setVersionTag("v2"))
            .setSortId("bbb");

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

    // First pass
    sideEffect
        .applyMCPSideEffect(Collections.singletonList(changeItem), retrieverContext)
        .collect(Collectors.toList());

    assertFalse(
        properties.hasIsLatest(),
        "Side effect must not mutate the incoming VersionProperties DataMap");

    // Second pass (simulated retry) — must succeed without error
    List<MCPItem> retryResults =
        sideEffect
            .applyMCPSideEffect(Collections.singletonList(changeItem), retrieverContext)
            .collect(Collectors.toList());

    assertEquals(retryResults.size(), 3, "Retry must produce the same three MCPs");

    Optional<MCPItem> setIsLatestMCP =
        retryResults.stream().filter(item -> item.getUrn().equals(ENTITY_URN)).findFirst();
    assertTrue(setIsLatestMCP.isPresent());
    assertTrue(setIsLatestMCP.get().getAspect(VersionProperties.class).isIsLatest());
  }
}
