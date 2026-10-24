package com.linkedin.metadata.ingestion.validation;

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
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionSourceRunSummaryValidatorTest {

  private static final OperationFingerprint SYSTEM =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final Urn SOURCE_URN = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-a");
  private static final Urn OLD_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-old");
  private static final Urn NEW_RUN = UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run-new");

  private final IngestionSourceRunSummaryValidator validator =
      new IngestionSourceRunSummaryValidator()
          .setConfig(IngestionSourceRunSummaryValidator.pluginConfig());

  private CachingAspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    aspectRetriever = mock(CachingAspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(new TestEntityRegistry());
    sourceExists(true);
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(aspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
  }

  @Test
  public void testNonSystemWriteIsRejected() {
    List<AspectValidationException> errors =
        validator
            .validateProposed(
                OperationFingerprint.EMPTY,
                List.of(write(ChangeType.UPSERT, summary(NEW_RUN, 200), null)),
                retrieverContext,
                null)
            .toList();

    assertEquals(errors.size(), 1);
    assertEquals(errors.get(0).getSubType(), ValidationSubType.AUTHORIZATION);
  }

  @Test
  public void testSystemWriteIsAllowed() {
    assertTrue(
        validator
            .validateProposed(
                SYSTEM,
                List.of(write(ChangeType.UPSERT, summary(NEW_RUN, 200), null)),
                retrieverContext,
                null)
            .toList()
            .isEmpty());
  }

  @Test
  public void testNewerRunIsWritten() {
    assertTrue(
        preCommit(write(ChangeType.UPSERT, summary(NEW_RUN, 200), summary(OLD_RUN, 100)))
            .isEmpty());
  }

  @Test
  public void testOlderRunIsFiltered() {
    assertFiltered(
        preCommit(write(ChangeType.UPSERT, summary(OLD_RUN, 100), summary(NEW_RUN, 200))));
  }

  @Test
  public void testMissingSourceIsFiltered() {
    sourceExists(false);

    assertFiltered(preCommit(write(ChangeType.UPSERT, summary(NEW_RUN, 200), null)));
  }

  private List<AspectValidationException> preCommit(ChangeMCP item) {
    return validator.validatePreCommit(SYSTEM, List.of(item), retrieverContext).toList();
  }

  private static void assertFiltered(List<AspectValidationException> errors) {
    assertEquals(errors.size(), 1);
    assertEquals(errors.get(0).getSubType(), ValidationSubType.FILTER);
  }

  private void sourceExists(boolean exists) {
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(
            exists
                ? Map.of(SOURCE_URN, Map.of(INGESTION_INFO_ASPECT_NAME, new Aspect()))
                : Map.of());
  }

  private ChangeMCP write(
      ChangeType changeType,
      DataHubIngestionSourceRunSummary summary,
      @Nullable DataHubIngestionSourceRunSummary previous) {
    SystemAspect previousAspect = previous != null ? mock(SystemAspect.class) : null;
    if (previousAspect != null) {
      when(previousAspect.getRecordTemplate()).thenReturn(previous);
    }
    return ChangeItemImpl.builder()
        .changeType(changeType)
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
