package com.linkedin.datahub.upgrade.system.ingestion;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
            any(com.linkedin.mxe.MetadataChangeProposal.class),
            any(com.linkedin.common.AuditStamp.class),
            eq(false));
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }
}
