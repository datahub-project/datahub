package com.linkedin.gms.factory.entity.update.indices;

import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.EntityClientUpgradeResultStore;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

/**
 * Tests the persist behavior of the dual-write start time callback created by the factory. The
 * factory reaches storage through {@link SystemEntityClient}, not {@code EntityService}, so that
 * dual-write also works in the standalone MAE consumer, which has no datasource.
 */
public class UpdateIndicesUpgradeStrategyFactoryTest {

  private static final String UPGRADE_VERSION = "0.14.0-0";

  @Test
  public void testPersistDualWriteStartTimeCallsEntityClientIngestProposal() throws Exception {
    SystemEntityClient entityClient = mock(SystemEntityClient.class);
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    Urn upgradeIdUrn = BootstrapStep.getUpgradeUrn("BuildIndicesIncremental_" + UPGRADE_VERSION);

    // Set up existing Phase 1 state that the callback will read and update
    Map<String, String> existingState = new HashMap<>();
    existingState.put("datasetindex_v2.nextIndexName", "datasetindex_v2_next_123");
    existingState.put("datasetindex_v2.status", "COMPLETED");
    existingState.put("datasetindex_v2.reindexStartTime", "1000");

    DataHubUpgradeResult existingResult = new DataHubUpgradeResult();
    existingResult.setState(DataHubUpgradeState.SUCCEEDED);
    existingResult.setResult(new StringMap(existingState));

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(existingResult.data()));
    envelopedAspect.setSystemMetadata(new SystemMetadata().setVersion("1"));

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put("dataHubUpgradeResult", envelopedAspect);
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(aspects);

    when(entityClient.batchGetV2NoCache(
            eq(opContext), any(), eq(Set.of(upgradeIdUrn)), any(Set.class)))
        .thenReturn(Map.of(upgradeIdUrn, entityResponse));

    DataHubUpgradeResultConditionalPersist.mergeAndPersist(
        opContext,
        new EntityClientUpgradeResultStore(entityClient),
        upgradeIdUrn,
        DataHubUpgradeResultConditionalPersist.putResultEntry(
            "datasetindex_v2.dualWriteStartTime", "1500", null),
        DataHubUpgradeResultConditionalPersist.CLIENT_MAX_ATTEMPTS);

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityClient).ingestProposal(eq(opContext), mcpCaptor.capture(), anyBoolean());

    MetadataChangeProposal capturedMcp = mcpCaptor.getValue();
    assertTrue(capturedMcp.getEntityUrn().toString().contains("BuildIndicesIncremental"));
    assertEquals(capturedMcp.getHeaders().get(HTTP_HEADER_IF_VERSION_MATCH), "1");
  }
}
