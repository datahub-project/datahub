package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.CORP_USER_EDITABLE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.entity.CeilingDeleteH2Harness.editable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.identity.CorpUserEditableInfo;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** I6: an upsert racing the final delete transaction is never lost (layer 4, latch-driven). */
public class EbeanCeilingDeleteConcurrencyTest {
  private static final String EDITABLE = CORP_USER_EDITABLE_INFO_ASPECT_NAME;
  private ExecutorService writer;

  @DataProvider(name = "lockingModes")
  public Object[][] lockingModes() {
    return new Object[][] {{false}, {true}};
  }

  @BeforeMethod
  public void startWriter() {
    writer = Executors.newSingleThreadExecutor();
  }

  @AfterMethod(alwaysRun = true)
  public void stopWriter() {
    if (writer != null) {
      writer.shutdownNow();
    }
  }

  private static String aboutMe(CeilingDeleteH2Harness h, Urn urn) {
    return new CorpUserEditableInfo(
            h.entityService.getLatestAspect(h.opContext, urn, EDITABLE).data())
        .getAboutMe();
  }

  /** The writer commits after the delete transaction began but before its first locked read. */
  @Test(dataProvider = "lockingModes")
  public void upsertCommittedBeforeTheLockedReadSurvivesAsNewer(boolean optimisticLocking)
      throws Exception {
    final CeilingDeleteH2Harness h = CeilingDeleteH2Harness.build(optimisticLocking);
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-race-before-" + optimisticLocking);
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    h.upsert(urn, EDITABLE, editable("v1"), 1_000L);
    final DeleteCeiling ceiling =
        h.entityService.captureDeleteCeiling(h.opContext, urn, 1_500L).orElseThrow();
    doAnswer(
            invocation -> {
              writer
                  .submit(() -> h.upsert(urn, EDITABLE, editable("racing"), 2_000L))
                  .get(30, TimeUnit.SECONDS);
              return invocation.callRealMethod();
            })
        .when(h.aspectDao)
        .getLatestAspectForDecision(
            any(), eq(urn.toString()), eq(h.opContext.getKeyAspectName(urn)));

    final RollbackRunResult result = h.entityService.deleteUrn(h.opContext, urn, ceiling);

    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.PARTIAL);
    assertEquals(aboutMe(h, urn), "racing");
    assertEquals(h.storedVersion(urn, EDITABLE, 0L), 2L);
    assertNull(h.entityService.getLatestAspect(h.opContext, urn, STATUS_ASPECT_NAME));
  }

  /** The writer starts once every latest row is locked: it can only commit after the delete. */
  @Test(dataProvider = "lockingModes")
  public void upsertBlockedByTheLocksSurvivesAsAFreshRow(boolean optimisticLocking)
      throws Exception {
    final CeilingDeleteH2Harness h = CeilingDeleteH2Harness.build(optimisticLocking);
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-race-after-" + optimisticLocking);
    h.upsert(urn, STATUS_ASPECT_NAME, new Status().setRemoved(true), 1_000L);
    h.upsert(urn, EDITABLE, editable("v1"), 1_000L);
    final DeleteCeiling ceiling =
        h.entityService.captureDeleteCeiling(h.opContext, urn, 1_500L).orElseThrow();
    final AtomicReference<Future<?>> racing = new AtomicReference<>();
    doAnswer(
            invocation -> {
              final Object locked = invocation.callRealMethod();
              racing.set(writer.submit(() -> h.upsert(urn, EDITABLE, editable("racing"), 2_000L)));
              return locked;
            })
        .when(h.aspectDao)
        .getLatestAspectsForDecision(any(), eq(urn));

    final RollbackRunResult result = h.entityService.deleteUrn(h.opContext, urn, ceiling);
    racing.get().get(30, TimeUnit.SECONDS);

    // The delete decided on rows the writer could not change; the writer then found the aspect
    // gone and wrote a fresh row (versions restart): a write after the delete, as today.
    assertEquals(result.getConditionalDeleteOutcome(), ConditionalDeleteOutcome.DELETED);
    assertEquals(aboutMe(h, urn), "racing");
    assertEquals(h.storedVersion(urn, EDITABLE, 0L), 1L);
  }
}
