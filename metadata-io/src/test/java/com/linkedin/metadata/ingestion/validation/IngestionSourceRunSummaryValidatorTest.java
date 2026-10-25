package com.linkedin.metadata.ingestion.validation;

import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceRunSummary;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationSubType;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionSourceRunSummaryValidatorTest {

  private static final OperationFingerprint SYSTEM =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final Urn SOURCE_URN = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-a");
  private static final Urn OTHER_SOURCE_URN =
      UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-b");
  private static final Urn OLD_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-old");
  private static final Urn NEW_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-new");

  private final IngestionSourceRunSummaryValidator validator =
      new IngestionSourceRunSummaryValidator()
          .setConfig(IngestionSourceRunSummaryValidator.pluginConfig());

  private final Map<Urn, Map<String, Aspect>> sources = new HashMap<>();
  private final Map<Urn, Map<String, Aspect>> runs = new HashMap<>();
  private CachingAspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    sources.clear();
    runs.clear();
    sources.put(SOURCE_URN, Map.of(INGESTION_INFO_ASPECT_NAME, new Aspect()));
    storeRun(OLD_RUN, SOURCE_URN, 100, null);
    storeRun(NEW_RUN, SOURCE_URN, 200, null);

    aspectRetriever = mock(CachingAspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(new TestEntityRegistry());
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<String> aspectNames = invocation.getArgument(2);
              return aspectNames.contains(INGESTION_INFO_ASPECT_NAME) ? sources : runs;
            });
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(aspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
  }

  @Test
  public void testAnyWriterPassesProposal() {
    assertTrue(
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                List.of(write(summary(NEW_RUN, 200), null)),
                retrieverContext,
                null)
            .toList()
            .isEmpty());
  }

  @Test
  public void testNewerRunIsWritten() {
    assertTrue(preCommit(write(summary(NEW_RUN, 200), summary(OLD_RUN, 100))).isEmpty());
  }

  @Test
  public void testOlderRunIsFiltered() {
    assertFiltered(preCommit(write(summary(OLD_RUN, 100), summary(NEW_RUN, 200))));
  }

  @Test
  public void testMissingSourceIsFiltered() {
    sources.clear();

    assertFiltered(preCommit(write(summary(NEW_RUN, 200), null)));
  }

  @Test
  public void testSummaryNotMatchingItsRunIsFiltered() {
    assertFiltered(preCommit(write(summary(NEW_RUN, Long.MAX_VALUE), null)));
  }

  @Test
  public void testRunForAnotherSourceIsFiltered() {
    storeRun(NEW_RUN, OTHER_SOURCE_URN, 200, null);

    assertFiltered(preCommit(write(summary(NEW_RUN, 200), null)));
  }

  @Test
  public void testStaleStatusIsFiltered() {
    storeRun(NEW_RUN, SOURCE_URN, 200, "SUCCESS");

    assertFiltered(preCommit(write(summary(NEW_RUN, 200).setLastRunStatus("RUNNING"), null)));
    assertTrue(preCommit(write(summary(NEW_RUN, 200).setLastRunStatus("SUCCESS"), null)).isEmpty());
  }

  private List<AspectValidationException> preCommit(ChangeMCP item) {
    return validator.validatePreCommit(SYSTEM, List.of(item), retrieverContext).toList();
  }

  private static void assertFiltered(List<AspectValidationException> errors) {
    assertEquals(errors.size(), 1);
    assertEquals(errors.get(0).getSubType(), ValidationSubType.FILTER);
  }

  private void storeRun(Urn runUrn, Urn sourceUrn, long requestedAt, @Nullable String status) {
    ExecutionRequestInput input =
        new ExecutionRequestInput()
            .setTask("RUN_INGEST")
            .setExecutorId("default")
            .setRequestedAt(requestedAt)
            .setArgs(new StringMap())
            .setSource(
                new ExecutionRequestSource()
                    .setType("SCHEDULED_INGESTION_SOURCE")
                    .setIngestionSource(sourceUrn));
    Map<String, Aspect> run = new HashMap<>();
    run.put(EXECUTION_REQUEST_INPUT_ASPECT_NAME, new Aspect(input.data()));
    if (status != null) {
      run.put(
          EXECUTION_REQUEST_RESULT_ASPECT_NAME,
          new Aspect(new ExecutionRequestResult().setStatus(status).data()));
    }
    runs.put(runUrn, run);
  }

  private ChangeMCP write(
      DataHubIngestionSourceRunSummary summary,
      @Nullable DataHubIngestionSourceRunSummary previous) {
    SystemAspect previousAspect = previous != null ? mock(SystemAspect.class) : null;
    if (previousAspect != null) {
      when(previousAspect.getRecordTemplate()).thenReturn(previous);
    }
    return ChangeItemImpl.builder()
        .changeType(ChangeType.UPSERT)
        .urn(SOURCE_URN)
        .aspectName(INGESTION_SOURCE_RUN_SUMMARY_ASPECT_NAME)
        .recordTemplate(summary)
        .previousSystemAspect(previousAspect)
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build(aspectRetriever);
  }

  private static DataHubIngestionSourceRunSummary summary(Urn runUrn, long requestedAt) {
    return new DataHubIngestionSourceRunSummary()
        .setLastExecutionRequest(runUrn)
        .setLastRunRequestedAtMillis(requestedAt);
  }
}
