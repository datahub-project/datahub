package com.linkedin.metadata.entity.upgrade;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.ErrorResponse;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Covers the {@link SystemEntityClient}-backed store, which is the path the standalone MAE consumer
 * takes — it runs {@code entityClient.impl=restli} and has no datasource.
 */
public class DataHubUpgradeResultStoreTest {

  private static final Urn UPGRADE_URN =
      UrnUtils.getUrn("urn:li:dataHubUpgrade:BuildIndicesIncremental_test");
  private static final String VERSION_CONFLICT_MESSAGE =
      "Failed to validate MCP due to: ValidationExceptionCollection{EntityAspect:"
          + "(dataHubUpgrade, dataHubUpgradeResult) Exceptions: [PRECONDITION "
          + "Expected version 1, actual version 2]}";

  private SystemEntityClient entityClient;
  private OperationContext opContext;

  @BeforeMethod
  public void setup() {
    entityClient = mock(SystemEntityClient.class);
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  /**
   * A cached read would hand back a stale {@code systemMetadata.version}, making every conditional
   * write conflict, and would hide swap-state transitions from the dual-write poller.
   */
  @Test
  public void testReadLatestBypassesTheClientCache() throws Exception {
    when(entityClient.batchGetV2NoCache(
            eq(opContext),
            eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME),
            eq(Set.of(UPGRADE_URN)),
            eq(Set.of(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME))))
        .thenReturn(Map.of(UPGRADE_URN, upgradeResponse("7")));

    EnvelopedAspect result =
        new EntityClientUpgradeResultStore(entityClient).readLatest(opContext, UPGRADE_URN);

    assertEquals(result.getSystemMetadata().getVersion(), "7");
    verify(entityClient, never()).getV2(any(), any(Urn.class), any());
  }

  @Test
  public void testReadLatestReturnsNullWhenAbsent() throws Exception {
    when(entityClient.batchGetV2NoCache(any(), any(), any(), any())).thenReturn(Map.of());

    assertNull(new EntityClientUpgradeResultStore(entityClient).readLatest(opContext, UPGRADE_URN));
  }

  /**
   * {@code aspects} is a required PDL field, so the default STRICT getter throws rather than
   * returning null. An absent aspect has to read as absent: {@code mergeAndPersist} calls {@code
   * readLatest} inside its compare-and-set loop with no catch, so a throw here would abort the
   * write instead of creating the aspect.
   */
  @Test
  public void testReadLatestTreatsAResponseWithoutAspectsAsAbsent() throws Exception {
    when(entityClient.batchGetV2NoCache(any(), any(), any(), any()))
        .thenReturn(Map.of(UPGRADE_URN, new EntityResponse()));

    assertNull(new EntityClientUpgradeResultStore(entityClient).readLatest(opContext, UPGRADE_URN));
  }

  /**
   * Restli erases the exception type: {@code AspectResource} maps a failed precondition to HTTP 422
   * carrying the validator's message. The store has to recognise that and re-raise it as the {@code
   * ValidationException} the retry loop understands, or a version conflict would abort instead of
   * retrying.
   */
  @Test
  public void testVersionConflictOverTheWireIsRetried() throws Exception {
    when(entityClient.batchGetV2NoCache(any(), any(), any(), any()))
        .thenReturn(Map.of(UPGRADE_URN, upgradeResponse("1")));
    when(entityClient.ingestProposal(any(), any(), anyBoolean()))
        .thenThrow(restLiException(422, VERSION_CONFLICT_MESSAGE))
        .thenReturn(UPGRADE_URN.toString());

    persist(DataHubUpgradeResultConditionalPersist.CLIENT_MAX_ATTEMPTS);

    verify(entityClient, times(2)).ingestProposal(any(), any(), anyBoolean());
  }

  /**
   * The in-process transport ({@code SystemJavaEntityClient}) hands back {@code EntityService}'s
   * {@link ValidationException} unwrapped, so the conflict must be recognised without any restli
   * involvement.
   */
  @Test
  public void testVersionConflictInProcessIsRetried() throws Exception {
    when(entityClient.batchGetV2NoCache(any(), any(), any(), any()))
        .thenReturn(Map.of(UPGRADE_URN, upgradeResponse("1")));
    when(entityClient.ingestProposal(any(), any(), anyBoolean()))
        .thenThrow(new ValidationException(VERSION_CONFLICT_MESSAGE))
        .thenReturn(UPGRADE_URN.toString());

    persist(DataHubUpgradeResultConditionalPersist.CLIENT_MAX_ATTEMPTS);

    verify(entityClient, times(2)).ingestProposal(any(), any(), anyBoolean());
  }

  /**
   * On the in-process path the exception must be re-raised as-is, not rebuilt from its message —
   * rebuilding would drop the {@code ValidationExceptionCollection} that callers use to classify
   * validation subtypes.
   */
  @Test
  public void testInProcessExceptionIsPropagatedUnchanged() throws Exception {
    ValidationException original = new ValidationException(VERSION_CONFLICT_MESSAGE);

    when(entityClient.batchGetV2NoCache(any(), any(), any(), any()))
        .thenReturn(Map.of(UPGRADE_URN, upgradeResponse("1")));
    when(entityClient.ingestProposal(any(), any(), anyBoolean())).thenThrow(original);

    ValidationException thrown = expectThrows(ValidationException.class, () -> persist(1));

    assertSame(thrown, original);
  }

  /**
   * The marker can sit on a nested cause rather than the outer exception. Recognising the conflict
   * but then rebuilding it from the outer message produced a {@code ValidationException} the retry
   * loop no longer matched, so the conflict aborted instead of retrying.
   */
  @Test
  public void testVersionConflictOnANestedCauseIsRetried() throws Exception {
    when(entityClient.batchGetV2NoCache(any(), any(), any(), any()))
        .thenReturn(Map.of(UPGRADE_URN, upgradeResponse("1")));
    when(entityClient.ingestProposal(any(), any(), anyBoolean()))
        .thenThrow(
            new RemoteInvocationException(
                "failed to ingest", new IllegalStateException(VERSION_CONFLICT_MESSAGE)))
        .thenReturn(UPGRADE_URN.toString());

    persist(DataHubUpgradeResultConditionalPersist.CLIENT_MAX_ATTEMPTS);

    verify(entityClient, times(2)).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testUnrelatedFailureIsNotRetried() throws Exception {
    when(entityClient.batchGetV2NoCache(any(), any(), any(), any()))
        .thenReturn(Map.of(UPGRADE_URN, upgradeResponse("1")));
    when(entityClient.ingestProposal(any(), any(), anyBoolean()))
        .thenThrow(new RemoteInvocationException("connection refused"));

    assertThrows(
        RemoteInvocationException.class,
        () -> persist(DataHubUpgradeResultConditionalPersist.CLIENT_MAX_ATTEMPTS));

    verify(entityClient, times(1)).ingestProposal(any(), any(), anyBoolean());
  }

  private void persist(int maxAttempts) throws Exception {
    DataHubUpgradeResultConditionalPersist.mergeAndPersist(
        opContext,
        new EntityClientUpgradeResultStore(entityClient),
        UPGRADE_URN,
        DataHubUpgradeResultConditionalPersist.putResultEntry("k", "v", null),
        maxAttempts);
  }

  private static EntityResponse upgradeResponse(String version) {
    DataHubUpgradeResult result = new DataHubUpgradeResult();
    result.setState(DataHubUpgradeState.SUCCEEDED);
    result.setResult(new StringMap(Map.of("datasetindex_v2.status", "COMPLETED")));

    EnvelopedAspect aspect = new EnvelopedAspect();
    aspect.setValue(new Aspect(result.data()));
    aspect.setSystemMetadata(new SystemMetadata().setVersion(version));

    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, aspect);

    EntityResponse response = new EntityResponse();
    response.setAspects(aspects);
    return response;
  }

  private static RestLiResponseException restLiException(int status, String message) {
    ErrorResponse errorResponse = new ErrorResponse().setStatus(status).setMessage(message);
    return new RestLiResponseException(errorResponse);
  }
}
