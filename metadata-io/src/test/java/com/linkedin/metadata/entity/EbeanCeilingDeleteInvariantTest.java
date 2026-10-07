package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.CORP_USER_EDITABLE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.mxe.MetadataChangeLog;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Layer 5: fixed seeds generate an entity (random history depth per aspect), capture its ceiling,
 * write random newer versions and new aspects, delete, then check I1-I5 against the model. A
 * failure names its seed; re-run that seed alone to reproduce.
 */
public class EbeanCeilingDeleteInvariantTest {
  private static final List<String> POOL =
      List.of(STATUS_ASPECT_NAME, CORP_USER_EDITABLE_INFO_ASPECT_NAME, GLOBAL_TAGS_ASPECT_NAME);

  @DataProvider(name = "seeds")
  public Object[][] seeds() {
    return new Object[][] {
      // seed, optimistic locking (alternating, so both modes see both outcomes)
      {11L, false}, {23L, true}, {37L, false}, {41L, true}, {53L, false}, {67L, true},
      {79L, false}, {83L, true}, {97L, false}, {101L, true}, {113L, false}, {127L, true}
    };
  }

  /** The n-th distinct value of an aspect (consecutive values always differ). */
  private static RecordTemplate value(String aspectName, int n) {
    if (STATUS_ASPECT_NAME.equals(aspectName)) {
      return new Status().setRemoved(n % 2 == 1);
    }
    if (CORP_USER_EDITABLE_INFO_ASPECT_NAME.equals(aspectName)) {
      return CeilingDeleteH2Harness.editable("v" + n);
    }
    return new GlobalTags()
        .setTags(new TagAssociationArray(new TagAssociation().setTag(new TagUrn("t" + n))));
  }

  @Test(dataProvider = "seeds")
  public void invariantsHoldForEverySeed(long seed, boolean optimisticLocking) {
    final String at = "seed " + seed + " optimisticLocking=" + optimisticLocking + ": ";
    final CeilingDeleteH2Harness h = CeilingDeleteH2Harness.build(optimisticLocking);
    final Random random = new Random(seed);
    final Urn urn = UrnUtils.getUrn("urn:li:corpuser:ceiling-seed-" + seed);
    final Map<String, Integer> before = new LinkedHashMap<>();
    final Map<String, Integer> after = new LinkedHashMap<>();
    long time = 1_000L;
    for (String aspect : POOL) {
      // status always exists before the capture, so the entity does.
      final int depth =
          STATUS_ASPECT_NAME.equals(aspect) ? 1 + random.nextInt(3) : random.nextInt(4);
      before.put(aspect, depth);
      for (int n = 1; n <= depth; n++) {
        h.upsert(urn, aspect, value(aspect, n), time++);
      }
    }
    final DeleteCeiling ceiling =
        h.entityService.captureDeleteCeiling(h.opContext, urn, time++).orElseThrow();
    for (String aspect : POOL) {
      final int extra = random.nextInt(3);
      after.put(aspect, extra);
      for (int n = before.get(aspect) + 1; n <= before.get(aspect) + extra; n++) {
        h.upsert(urn, aspect, value(aspect, n), time++);
      }
    }
    clearInvocations(h.producer);

    final RollbackRunResult result = h.entityService.deleteUrn(h.opContext, urn, ceiling);

    final boolean anySurvivor = after.values().stream().anyMatch(extra -> extra > 0);
    // I4
    assertEquals(
        result.getConditionalDeleteOutcome(),
        anySurvivor ? ConditionalDeleteOutcome.PARTIAL : ConditionalDeleteOutcome.DELETED,
        at + "outcome");
    assertEquals(
        h.entityService.captureDeleteCeiling(h.opContext, urn, time).isPresent(),
        anySurvivor,
        at + "key present");
    for (String aspect : POOL) {
      final int b = before.get(aspect);
      final int a = after.get(aspect);
      final String where = at + aspect + " before=" + b + " after=" + a + ": ";
      if (b + a == 0) {
        continue;
      }
      if (a == 0) {
        // I2: everything at or below the ceiling is gone.
        assertNull(
            h.aspectDao.getAspect(h.opContext, urn.toString(), aspect, 0L), where + "latest");
      } else {
        // I1 / I3: the newer latest survives with its version; history above the ceiling too.
        assertEquals(h.storedVersion(urn, aspect, 0L), (long) (b + a), where + "latest version");
        for (long v = b + 1; v < b + a; v++) {
          assertNotNull(
              h.aspectDao.getAspect(h.opContext, urn.toString(), aspect, v), where + "row " + v);
        }
      }
      // I2: history at or below the ceiling is gone in every case.
      for (long v = 1; v <= b; v++) {
        assertNull(
            h.aspectDao.getAspect(h.opContext, urn.toString(), aspect, v), where + "row " + v);
      }
    }
    // I5
    final ArgumentCaptor<MetadataChangeLog> mcls = ArgumentCaptor.forClass(MetadataChangeLog.class);
    verify(h.producer, atLeast(0)).produceMetadataChangeLog(any(), any(), any(), mcls.capture());
    final long keyDeletes =
        mcls.getAllValues().stream().filter(mcl -> h.isKeyDelete(mcl, urn)).count();
    assertEquals(keyDeletes, anySurvivor ? 0L : 1L, at + "key DELETE MCLs");
    for (String aspect : POOL) {
      final boolean latestRemovedAlone =
          anySurvivor && before.get(aspect) > 0 && after.get(aspect) == 0;
      final long aspectDeletes =
          mcls.getAllValues().stream()
              .filter(mcl -> CeilingDeleteH2Harness.isAspectDelete(mcl, aspect))
              .count();
      assertEquals(aspectDeletes, latestRemovedAlone ? 1L : 0L, at + aspect + " DELETE MCLs");
    }
  }
}
