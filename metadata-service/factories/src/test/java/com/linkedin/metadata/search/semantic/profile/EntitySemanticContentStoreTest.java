package com.linkedin.metadata.search.semantic.profile;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.patch.template.common.SemanticProfileConflictException;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class EntitySemanticContentStoreTest {

  private static final Urn URN = UrnUtils.getUrn("urn:li:document:one");

  @Test
  public void testExistsReadsThroughTheAspectRetriever() {
    EntityService<?> entityService = mock(EntityService.class);
    AspectRetriever retriever = mock(AspectRetriever.class);
    OperationContext context = mock(OperationContext.class);
    when(context.getAspectRetriever()).thenReturn(retriever);
    when(retriever.entityExists(context, Set.of(URN))).thenReturn(Map.of(URN, true));

    assertTrue(store(entityService).exists(context, URN));
    verifyNoInteractions(entityService);
  }

  @Test
  public void testAWrappedProfileConflictIsReportedAsNotWritten() {
    EntityService<?> entityService = mock(EntityService.class);
    when(entityService.ingestProposal(
            any(), any(MetadataChangeProposal.class), any(), anyBoolean()))
        .thenThrow(
            new RuntimeException(
                "Error performing JSON PATCH",
                new SemanticProfileConflictException("/embeddings/a/contentHash")));

    assertFalse(store(entityService).patch(systemContext(), URN, new MetadataChangeProposal()));
  }

  @Test
  public void testAnUncommittedWriteIsReportedAsNotWritten() {
    EntityService<?> entityService = mock(EntityService.class);
    when(entityService.ingestProposal(
            any(), any(MetadataChangeProposal.class), any(), anyBoolean()))
        .thenReturn(IngestResult.builder().urn(URN).sqlCommitted(false).build());

    assertFalse(store(entityService).patch(systemContext(), URN, new MetadataChangeProposal()));
  }

  @Test
  public void testACommittedWriteIsReportedAsWritten() {
    EntityService<?> entityService = mock(EntityService.class);
    when(entityService.ingestProposal(
            any(), any(MetadataChangeProposal.class), any(), anyBoolean()))
        .thenReturn(IngestResult.builder().urn(URN).sqlCommitted(true).build());

    assertTrue(store(entityService).patch(systemContext(), URN, new MetadataChangeProposal()));
  }

  @Test
  public void testAUserCallerIngestsAsTheSystem() {
    EntityService<?> entityService = mock(EntityService.class);
    when(entityService.ingestProposal(
            any(), any(MetadataChangeProposal.class), any(), anyBoolean()))
        .thenReturn(IngestResult.builder().urn(URN).sqlCommitted(true).build());
    OperationContext user =
        TestOperationContexts.userContextNoSearchAuthorization(
            UrnUtils.getUrn("urn:li:corpuser:tester"));
    assertFalse(user.isSystemAuth());

    assertTrue(store(entityService).patch(user, URN, new MetadataChangeProposal()));

    ArgumentCaptor<OperationContext> captured = ArgumentCaptor.forClass(OperationContext.class);
    verify(entityService)
        .ingestProposal(captured.capture(), any(MetadataChangeProposal.class), any(), anyBoolean());
    assertTrue(captured.getValue().isSystemAuth());
  }

  @Test
  public void testOtherFailuresPropagate() {
    EntityService<?> entityService = mock(EntityService.class);
    when(entityService.ingestProposal(
            any(), any(MetadataChangeProposal.class), any(), anyBoolean()))
        .thenThrow(new IllegalStateException("database down"));

    assertThrows(
        IllegalStateException.class,
        () -> store(entityService).patch(systemContext(), URN, new MetadataChangeProposal()));
  }

  private static OperationContext systemContext() {
    return TestOperationContexts.systemContextNoSearchAuthorization();
  }

  private static EntitySemanticContentStore store(EntityService<?> entityService) {
    return new EntitySemanticContentStore(entityService);
  }
}
