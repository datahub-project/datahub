package com.linkedin.metadata.ingestion.validation;

import static com.linkedin.metadata.Constants.INGESTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;
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
import com.linkedin.ingestion.DataHubIngestionSourceRunEvent;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationSubType;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IngestionSourceRunEventValidatorTest {

  private static final Urn SOURCE_URN = UrnUtils.getUrn("urn:li:dataHubIngestionSource:source-a");
  private static final Urn DELETED_SOURCE_URN =
      UrnUtils.getUrn("urn:li:dataHubIngestionSource:deleted");

  private final IngestionSourceRunEventValidator validator =
      new IngestionSourceRunEventValidator()
          .setConfig(IngestionSourceRunEventValidator.pluginConfig());
  private final Map<Urn, Map<String, Aspect>> sources = new HashMap<>();
  private CachingAspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    sources.clear();
    sources.put(SOURCE_URN, Map.of(INGESTION_INFO_ASPECT_NAME, new Aspect()));
    aspectRetriever = mock(CachingAspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(new TestEntityRegistry());
    when(aspectRetriever.getLatestAspectObjects(any(), any(), any())).thenReturn(sources);
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mock(SearchRetriever.class))
            .cachingAspectRetriever(aspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
  }

  @Test
  public void testEventForExistingSourceIsAccepted() {
    assertTrue(validate(event(SOURCE_URN)).isEmpty());
  }

  @Test
  public void testEventForDeletedSourceIsRejected() {
    List<AspectValidationException> errors = validate(event(SOURCE_URN), event(DELETED_SOURCE_URN));

    assertEquals(errors.size(), 1);
    assertEquals(errors.get(0).getAspectGroup().getFirst(), DELETED_SOURCE_URN);
    assertEquals(errors.get(0).getSubType(), ValidationSubType.VALIDATION);
  }

  private List<AspectValidationException> validate(BatchItem... items) {
    return validator
        .validateProposed(OperationFingerprint.EMPTY, List.of(items), retrieverContext, null)
        .toList();
  }

  private BatchItem event(Urn sourceUrn) {
    return ChangeItemImpl.builder()
        .changeType(ChangeType.UPSERT)
        .urn(sourceUrn)
        .aspectName(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME)
        .recordTemplate(
            new DataHubIngestionSourceRunEvent()
                .setTimestampMillis(1L)
                .setExecutionRequest(UrnUtils.getUrn("urn:li:dataHubExecutionRequest:run"))
                .setRequestedAtMillis(1L)
                .setStatus("SUCCESS"))
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build(aspectRetriever);
  }
}
