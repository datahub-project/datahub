package com.linkedin.metadata.entity.versioning;

import static com.linkedin.metadata.Constants.INITIAL_VERSION_SORT_ID;
import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.FabricType;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceAspectRetriever;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.key.VersionSetKey;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistryException;
import com.linkedin.metadata.models.registry.MergedEntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.datahubproject.test.util.TestEntityRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityVersioningServiceTest {

  private EntityVersioningServiceImpl versioningService;
  private EntityService mockEntityService;
  private OperationContext mockOpContext;
  private AspectRetriever mockAspectRetriever;
  private GraphRetriever mockGraphRetriever;
  private static Urn TEST_VERSION_SET_URN = UrnUtils.getUrn("urn:li:versionSet:(123456,dataset)");
  private static Urn TEST_DATASET_URN =
      new DatasetUrn(new DataPlatformUrn("kafka"), "myDataset", FabricType.PROD);
  private static Urn TEST_DATASET_URN_2 =
      new DatasetUrn(new DataPlatformUrn("hive"), "myHiveDataset", FabricType.PROD);

  @BeforeMethod
  public void setup() throws EntityRegistryException {
    mockEntityService = mock(EntityService.class);
    final EntityRegistry snapshotEntityRegistry = new TestEntityRegistry();
    final EntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    final EntityRegistry testEntityRegistry =
        new MergedEntityRegistry(snapshotEntityRegistry).apply(configEntityRegistry);
    mockAspectRetriever = mock(EntityServiceAspectRetriever.class);
    mockGraphRetriever = mock(GraphRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(testEntityRegistry);
    mockOpContext =
        TestOperationContexts.systemContext(
            null,
            null,
            null,
            () -> testEntityRegistry,
            () ->
                RetrieverContext.builder()
                    .aspectRetriever(mockAspectRetriever)
                    .graphRetriever(mockGraphRetriever)
                    .searchRetriever(TestOperationContexts.emptySearchRetriever)
                    .build(),
            null,
            opContext ->
                ((EntityServiceAspectRetriever) opContext.getAspectRetrieverOpt().get())
                    .setSystemOperationContext(opContext),
            null);
    ;
    versioningService = new EntityVersioningServiceImpl(mockEntityService);
  }

  @Test
  public void testLinkLatestVersionNewVersionSet() throws Exception {

    VersionPropertiesInput input = new VersionPropertiesInput();
    input.setComment("Test comment");
    input.setLabel("Test label");
    input.setSourceCreationTimestamp(1234567890L);
    input.setSourceCreator("testCreator");

    // Mock version set doesn't exist
    when(mockAspectRetriever.entityExists(anySet()))
        .thenReturn(Map.of(TEST_VERSION_SET_URN, false));

    // Capture the proposals
    ArgumentCaptor<AspectsBatch> aspectsCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    when(mockEntityService.ingestProposal(eq(mockOpContext), aspectsCaptor.capture(), eq(false)))
        .thenReturn(List.of());

    // Execute
    versioningService.linkLatestVersion(
        mockOpContext, TEST_VERSION_SET_URN, TEST_DATASET_URN, input);

    // Verify
    List<AspectsBatch> capturedAspects = aspectsCaptor.getAllValues();
    List<RecordTemplate> versionPropertiesAspect =
        capturedAspects.get(0).getMCPItems().stream()
            .filter(mcpItem -> VERSION_PROPERTIES_ASPECT_NAME.equals(mcpItem.getAspectName()))
            .map(mcpItem -> mcpItem.getAspect(VersionProperties.class))
            .collect(Collectors.toList());

    // Verify VersionProperties has initial sort ID
    VersionProperties versionProps =
        (VersionProperties)
            versionPropertiesAspect.stream()
                .filter(a -> a instanceof VersionProperties)
                .findFirst()
                .orElseThrow(() -> new AssertionError("VersionProperties not found"));

    assertEquals(versionProps.getSortId(), INITIAL_VERSION_SORT_ID);
    assertEquals(versionProps.getComment(), "Test comment");
    assertEquals(versionProps.getVersionSet(), TEST_VERSION_SET_URN);

    List<RecordTemplate> versionSetKeyAspect =
        capturedAspects.get(0).getMCPItems().stream()
            .filter(mcpItem -> VERSION_SET_KEY_ASPECT_NAME.equals(mcpItem.getAspectName()))
            .map(mcpItem -> mcpItem.getAspect(VersionSetKey.class))
            .collect(Collectors.toList());

    VersionSetKey versionSetKey =
        (VersionSetKey)
            versionSetKeyAspect.stream()
                .filter(aspect -> aspect instanceof VersionSetKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Version Set Key not found"));
    assertEquals(versionSetKey.getId(), "123456");

    List<RecordTemplate> versionSetPropertiesAspect =
        capturedAspects.get(0).getMCPItems().stream()
            .filter(mcpItem -> VERSION_SET_PROPERTIES_ASPECT_NAME.equals(mcpItem.getAspectName()))
            .map(mcpItem -> mcpItem.getAspect(VersionSetProperties.class))
            .collect(Collectors.toList());
    VersionSetProperties versionSetProperties =
        (VersionSetProperties)
            versionSetPropertiesAspect.stream()
                .filter(aspect -> aspect instanceof VersionSetProperties)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Version Set Properties not found"));
    assertEquals(versionSetProperties.getLatest(), TEST_DATASET_URN);
    assertEquals(
        versionSetProperties.getVersioningScheme(),
        VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);
  }

  @Test
  public void testLinkLatestVersionExistingVersionSet() throws Exception {

    VersionPropertiesInput input = new VersionPropertiesInput();
    input.setComment("Test comment");
    input.setLabel("Label2");

    // Mock version set exists
    when(mockAspectRetriever.entityExists(anySet())).thenReturn(Map.of(TEST_VERSION_SET_URN, true));

    // Mock existing version set properties
    VersionSetProperties existingVersionSetProps =
        new VersionSetProperties()
            .setVersioningScheme(VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB)
            .setLatest(TEST_DATASET_URN);
    Aspect mockVersionSetPropertiesAspect = mock(Aspect.class);
    when(mockVersionSetPropertiesAspect.data()).thenReturn(existingVersionSetProps.data());
    when(mockAspectRetriever.getLatestAspectObject(eq(TEST_VERSION_SET_URN), anyString()))
        .thenReturn(mockVersionSetPropertiesAspect);

    // Mock existing version properties with a sort ID
    VersionProperties existingVersionProps =
        new VersionProperties()
            .setSortId("AAAAAAAA")
            .setLabel("Label1")
            .setVersionSet(TEST_VERSION_SET_URN);
    Aspect mockVersionPropertiesAspect = mock(Aspect.class);
    when(mockVersionPropertiesAspect.data()).thenReturn(existingVersionProps.data());
    when(mockAspectRetriever.getLatestAspectObject(eq(TEST_DATASET_URN), anyString()))
        .thenReturn(mockVersionPropertiesAspect);

    // Capture the proposals
    ArgumentCaptor<AspectsBatch> aspectsCaptor = ArgumentCaptor.forClass(AspectsBatch.class);
    when(mockEntityService.ingestProposal(eq(mockOpContext), aspectsCaptor.capture(), eq(false)))
        .thenReturn(List.of());

    // Execute
    versioningService.linkLatestVersion(
        mockOpContext, TEST_VERSION_SET_URN, TEST_DATASET_URN_2, input);

    // Verify
    List<AspectsBatch> capturedAspects = aspectsCaptor.getAllValues();
    List<RecordTemplate> aspects =
        capturedAspects.get(0).getMCPItems().stream()
            .filter(mcpItem -> VERSION_PROPERTIES_ASPECT_NAME.equals(mcpItem.getAspectName()))
            .map(mcpItem -> mcpItem.getAspect(VersionProperties.class))
            .collect(Collectors.toList());

    // Verify VersionProperties has incremented sort ID
    VersionProperties versionProps =
        (VersionProperties)
            aspects.stream()
                .filter(a -> a instanceof VersionProperties)
                .findFirst()
                .orElseThrow(() -> new AssertionError("VersionProperties not found"));

    assertEquals(versionProps.getSortId(), "AAAAAAAB");
    assertEquals(versionProps.getComment(), "Test comment");
    assertEquals(versionProps.getVersionSet(), TEST_VERSION_SET_URN);
  }

  @Test
  public void testUnlinkInitialVersion() throws Exception {

    // Mock version properties aspect
    VersionProperties versionProps =
        new VersionProperties()
            .setVersionSet(TEST_VERSION_SET_URN)
            .setSortId(INITIAL_VERSION_SORT_ID);
    Aspect mockVersionPropsAspect = mock(Aspect.class);
    when(mockVersionPropsAspect.data()).thenReturn(versionProps.data());
    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_DATASET_URN), eq(VERSION_PROPERTIES_ASPECT_NAME)))
        .thenReturn(mockVersionPropsAspect);

    // Mock delete aspect responses
    RollbackResult versionSetDeleteResult =
        new RollbackResult(
            TEST_VERSION_SET_URN,
            "versionSet",
            VERSION_SET_PROPERTIES_ASPECT_NAME,
            null,
            null,
            null,
            null,
            null,
            false,
            0);
    RollbackResult versionPropsDeleteResult =
        new RollbackResult(
            TEST_DATASET_URN,
            "dataset",
            VERSION_PROPERTIES_ASPECT_NAME,
            null,
            null,
            null,
            null,
            null,
            false,
            0);

    when(mockEntityService.deleteAspect(
            eq(mockOpContext),
            anyString(),
            eq(VERSION_SET_PROPERTIES_ASPECT_NAME),
            anyMap(),
            eq(true)))
        .thenReturn(Optional.of(versionSetDeleteResult));
    when(mockEntityService.deleteAspect(
            eq(mockOpContext), anyString(), eq(VERSION_PROPERTIES_ASPECT_NAME), anyMap(), eq(true)))
        .thenReturn(Optional.of(versionPropsDeleteResult));

    // Execute
    List<RollbackResult> results =
        versioningService.unlinkLatestVersion(mockOpContext, TEST_DATASET_URN);

    // Verify
    assertEquals(results.size(), 2);
    verify(mockEntityService)
        .deleteAspect(
            eq(mockOpContext),
            eq(TEST_VERSION_SET_URN.toString()),
            eq(VERSION_SET_PROPERTIES_ASPECT_NAME),
            anyMap(),
            eq(true));
    verify(mockEntityService)
        .deleteAspect(
            eq(mockOpContext),
            eq(TEST_DATASET_URN.toString()),
            eq(VERSION_PROPERTIES_ASPECT_NAME),
            anyMap(),
            eq(true));
    verify(mockGraphRetriever, never())
        .scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyLong(), anyLong());
  }

  @Test
  public void testUnlinkLatestVersionWithPriorVersion() throws Exception {

    // Mock version properties aspect
    VersionProperties versionProps =
        new VersionProperties()
            .setVersionSet(TEST_VERSION_SET_URN)
            .setSortId("AAAAAAAB"); // Not initial version
    Aspect mockVersionPropsAspect = mock(Aspect.class);
    when(mockVersionPropsAspect.data()).thenReturn(versionProps.data());
    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_DATASET_URN), eq(VERSION_PROPERTIES_ASPECT_NAME)))
        .thenReturn(mockVersionPropsAspect);

    // Mock graph retriever response
    List<RelatedEntities> relatedEntities = new ArrayList<>();
    relatedEntities.add(
        new RelatedEntities(
            "VersionOf",
            TEST_DATASET_URN.toString(),
            TEST_DATASET_URN.toString(),
            RelationshipDirection.INCOMING,
            null));
    relatedEntities.add(
        new RelatedEntities(
            "VersionOf",
            TEST_DATASET_URN_2.toString(),
            TEST_DATASET_URN_2.toString(),
            RelationshipDirection.INCOMING,
            null));

    RelatedEntitiesScrollResult scrollResult =
        new RelatedEntitiesScrollResult(2, 2, null, relatedEntities);
    when(mockGraphRetriever.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), eq(2), any(), any()))
        .thenReturn(scrollResult);

    // Mock delete aspect response
    RollbackResult versionPropsDeleteResult =
        new RollbackResult(
            TEST_DATASET_URN,
            "dataset",
            VERSION_PROPERTIES_ASPECT_NAME,
            null,
            null,
            null,
            null,
            null,
            false,
            0);
    when(mockEntityService.deleteAspect(
            eq(mockOpContext), anyString(), eq(VERSION_PROPERTIES_ASPECT_NAME), anyMap(), eq(true)))
        .thenReturn(Optional.of(versionPropsDeleteResult));

    // Execute
    List<RollbackResult> results =
        versioningService.unlinkLatestVersion(mockOpContext, TEST_DATASET_URN);

    // Verify
    assertEquals(results.size(), 1);
    verify(mockEntityService)
        .deleteAspect(
            eq(mockOpContext),
            eq(TEST_DATASET_URN.toString()),
            eq(VERSION_PROPERTIES_ASPECT_NAME),
            anyMap(),
            eq(true));
    verify(mockEntityService).ingestProposal(eq(mockOpContext), any(), eq(false));
    verify(mockEntityService, never())
        .deleteAspect(
            eq(mockOpContext),
            eq(TEST_VERSION_SET_URN.toString()),
            eq(VERSION_SET_PROPERTIES_ASPECT_NAME),
            anyMap(),
            eq(true));
  }

  @Test
  public void testUnlinkNonVersionedEntity() throws Exception {

    // Mock no version properties aspect
    when(mockAspectRetriever.getLatestAspectObject(
            eq(TEST_DATASET_URN), eq(VERSION_PROPERTIES_ASPECT_NAME)))
        .thenReturn(null);

    // Execute
    List<RollbackResult> results =
        versioningService.unlinkLatestVersion(mockOpContext, TEST_DATASET_URN);

    // Verify
    assertTrue(results.isEmpty());
    verify(mockEntityService, never()).deleteAspect(any(), any(), any(), any(), anyBoolean());
    verify(mockGraphRetriever, never())
        .scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyLong(), anyLong());
  }
}
