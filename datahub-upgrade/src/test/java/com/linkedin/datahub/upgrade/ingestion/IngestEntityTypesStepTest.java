package com.linkedin.datahub.upgrade.system.ingestion;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestEntityTypesStepTest {

  private static final Urn UPGRADE_ID_URN =
      UrnUtils.getUrn("urn:li:dataHubUpgrade:ingest-entity-types-v1");

  @Mock private OperationContext mockOpContext;
  @Mock private com.linkedin.metadata.entity.EntityService<?> mockEntityService;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private UpgradeContext mockUpgradeContext;

  @Captor private ArgumentCaptor<MetadataChangeProposal> proposalCaptor;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
  }

  @Test
  public void testIngestsOnlyMissingEntityTypes() throws Exception {
    // Build 5 entity specs
    Map<String, EntitySpec> entitySpecMap = new HashMap<>();
    for (int i = 1; i <= 5; i++) {
      EntitySpec spec = mock(EntitySpec.class);
      when(spec.getName()).thenReturn("entity" + i);
      entitySpecMap.put("entity" + i, spec);
    }
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecMap);

    // 2 already exist: entity1 and entity2
    Set<Urn> existingUrns =
        Set.of(
            UrnUtils.getUrn("urn:li:entityType:datahub.entity1"),
            UrnUtils.getUrn("urn:li:entityType:datahub.entity2"));
    when(mockEntityService.exists(eq(mockOpContext), anySet())).thenReturn(existingUrns);

    IngestEntityTypesStep step = new IngestEntityTypesStep(mockEntityService, true);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // 3 missing entity types ingested + 1 for setUpgradeResult = 4 total ingestProposal calls
    // setUpgradeResult internally calls ingestProposal once for the upgrade marker
    verify(mockEntityService, times(4))
        .ingestProposal(
            any(OperationContext.class),
            proposalCaptor.capture(),
            any(com.linkedin.common.AuditStamp.class),
            eq(false));
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify content of captured MCP proposals for missing entities
    var proposals = proposalCaptor.getAllValues();
    assertEquals(proposals.size(), 4);

    // First 3 proposals should be for entity3, entity4, entity5 (missing ones)
    for (int i = 0; i < 3; i++) {
      MetadataChangeProposal proposal = proposals.get(i);
      assertNotNull(proposal.getEntityUrn());
      assertEquals(proposal.getEntityType(), ENTITY_TYPE_ENTITY_NAME);
      assertEquals(proposal.getAspectName(), ENTITY_TYPE_INFO_ASPECT_NAME);
      assertEquals(proposal.getChangeType(), ChangeType.UPSERT);

      // Verify URN format: urn:li:entityType:datahub.entityX
      String urnStr = proposal.getEntityUrn().toString();
      assertNotNull(urnStr, "Entity URN should not be null");
      assertEquals(urnStr.startsWith("urn:li:entityType:datahub.entity"), true);

      // Verify aspect is set (serialized EntityTypeInfo)
      assertNotNull(proposal.getAspect());
    }
  }

  @Test
  public void testAllEntityTypesExist() throws Exception {
    Map<String, EntitySpec> entitySpecMap = new HashMap<>();
    for (int i = 1; i <= 3; i++) {
      EntitySpec spec = mock(EntitySpec.class);
      when(spec.getName()).thenReturn("entity" + i);
      entitySpecMap.put("entity" + i, spec);
    }
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecMap);

    // All exist
    Set<Urn> allExist =
        Set.of(
            UrnUtils.getUrn("urn:li:entityType:datahub.entity1"),
            UrnUtils.getUrn("urn:li:entityType:datahub.entity2"),
            UrnUtils.getUrn("urn:li:entityType:datahub.entity3"));
    when(mockEntityService.exists(eq(mockOpContext), anySet())).thenReturn(allExist);

    IngestEntityTypesStep step = new IngestEntityTypesStep(mockEntityService, true);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // Only 1 call for setUpgradeResult, no entity type ingestions
    verify(mockEntityService, times(1))
        .ingestProposal(
            any(OperationContext.class),
            any(MetadataChangeProposal.class),
            any(com.linkedin.common.AuditStamp.class),
            eq(false));
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testNoneExist() throws Exception {
    Map<String, EntitySpec> entitySpecMap = new HashMap<>();
    for (int i = 1; i <= 3; i++) {
      EntitySpec spec = mock(EntitySpec.class);
      when(spec.getName()).thenReturn("entity" + i);
      entitySpecMap.put("entity" + i, spec);
    }
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecMap);

    // None exist
    Set<Urn> noneExist = Set.of();
    when(mockEntityService.exists(eq(mockOpContext), anySet())).thenReturn(noneExist);

    IngestEntityTypesStep step = new IngestEntityTypesStep(mockEntityService, true);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // 3 for entities + 1 for setUpgradeResult = 4 total
    verify(mockEntityService, times(4))
        .ingestProposal(
            any(OperationContext.class),
            proposalCaptor.capture(),
            any(com.linkedin.common.AuditStamp.class),
            eq(false));
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify all 3 entity types have correct format
    var proposals = proposalCaptor.getAllValues();
    for (int i = 0; i < 3; i++) {
      MetadataChangeProposal proposal = proposals.get(i);
      assertEquals(proposal.getChangeType(), ChangeType.UPSERT);
      assertEquals(proposal.getEntityType(), ENTITY_TYPE_ENTITY_NAME);
      assertEquals(proposal.getAspectName(), ENTITY_TYPE_INFO_ASPECT_NAME);
    }
  }

  @Test
  public void testExecutionFailureReturnsFailedState() throws Exception {
    Map<String, EntitySpec> entitySpecMap = new HashMap<>();
    EntitySpec spec = mock(EntitySpec.class);
    when(spec.getName()).thenReturn("entity1");
    entitySpecMap.put("entity1", spec);
    when(mockEntityRegistry.getEntitySpecs()).thenReturn(entitySpecMap);

    // Simulate exception
    when(mockEntityService.exists(eq(mockOpContext), anySet()))
        .thenThrow(new RuntimeException("Test exception"));

    IngestEntityTypesStep step = new IngestEntityTypesStep(mockEntityService, true);

    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    // Should return FAILED state
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }
}
