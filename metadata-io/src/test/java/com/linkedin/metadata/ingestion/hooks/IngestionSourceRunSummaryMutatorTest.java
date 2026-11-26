package com.linkedin.metadata.ingestion.hooks;

import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

public class IngestionSourceRunSummaryMutatorTest {

  private static final Urn SOURCE = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-a");
  private static final Urn RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run");
  private static final Urn OLDER_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:older");

  private final EntityRegistry entityRegistry = TestOperationContexts.defaultEntityRegistry();
  private final IngestionSourceRunSummaryMutator mutator =
      new IngestionSourceRunSummaryMutator()
          .setConfig(IngestionSourceRunSummaryMutator.pluginConfig());

  @Test
  public void testOlderRunMergesIntoTheStoredSummary() {
    DataHubIngestionSourceRunSummary stored =
        summary(RUN, 200L, "SUCCESS").setLastSuccessfulRunRequestedAtMillis(200L);
    DataHubIngestionSourceRunSummary proposed =
        summary(OLDER_RUN, 100L, "FAILURE").setLastFailedRunRequestedAtMillis(100L);

    Pair<ChangeMCP, Boolean> result = mutate(stored, proposed);

    assertTrue(result.getSecond());
    DataHubIngestionSourceRunSummary written =
        result.getFirst().getAspect(DataHubIngestionSourceRunSummary.class);
    assertEquals(written.getLastExecutionRequest(), RUN);
    assertEquals(written.getLastSuccessfulRunRequestedAtMillis(), Long.valueOf(200L));
    assertEquals(written.getLastFailedRunRequestedAtMillis(), Long.valueOf(100L));
  }

  @Test
  public void testFirstSummaryIsUnchanged() {
    assertFalse(mutate(null, summary(RUN, 100L, "PENDING")).getSecond());
  }

  private Pair<ChangeMCP, Boolean> mutate(
      @Nullable DataHubIngestionSourceRunSummary stored,
      DataHubIngestionSourceRunSummary proposed) {
    SystemAspect previous = null;
    if (stored != null) {
      previous = mock(SystemAspect.class);
      when(previous.getRecordTemplate()).thenReturn(stored);
    }
    ChangeMCP item =
        TestMCP.builder()
            .urn(SOURCE)
            .changeType(ChangeType.UPSERT)
            .entitySpec(entityRegistry.getEntitySpec(SOURCE.getEntityType()))
            .aspectSpec(
                entityRegistry
                    .getEntitySpec(SOURCE.getEntityType())
                    .getAspectSpec(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME))
            .recordTemplate(proposed)
            .previousSystemAspect(previous)
            .build();
    List<Pair<ChangeMCP, Boolean>> results =
        mutator
            .applyWriteMutation(
                OperationFingerprint.EMPTY, List.of(item), mock(RetrieverContext.class))
            .toList();
    assertEquals(results.size(), 1);
    return results.get(0);
  }

  private static DataHubIngestionSourceRunSummary summary(
      Urn run, long requestedAt, String status) {
    return new DataHubIngestionSourceRunSummary()
        .setLastExecutionRequest(run)
        .setLastRunRequestedAtMillis(requestedAt)
        .setLastRunStatus(status);
  }
}
