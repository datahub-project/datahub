package com.linkedin.metadata.entity;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class DeleteCeilingTest {

  @Test
  public void keyCreatedPrefersAspectCreatedOverRowCreatedOn() {
    SystemAspect key = mock(SystemAspect.class);
    when(key.getSystemMetadata())
        .thenReturn(
            new SystemMetadata()
                .setAspectCreated(
                    new AuditStamp()
                        .setTime(1_000L)
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"))));
    when(key.getCreatedOn()).thenReturn(new Timestamp(9_000L));

    assertEquals(DeleteCeiling.keyCreatedMillisOf(key), 1_000L);
  }

  @Test
  public void keyCreatedFallsBackToRowCreatedOnForLegacyRows() {
    SystemAspect key = mock(SystemAspect.class);
    when(key.getSystemMetadata()).thenReturn(new SystemMetadata());
    when(key.getCreatedOn()).thenReturn(new Timestamp(9_000L));

    assertEquals(DeleteCeiling.keyCreatedMillisOf(key), 9_000L);
  }

  @Test
  public void callerVersionsMustEqualTheCaptureExactly() {
    DeleteCeiling ceiling =
        new DeleteCeiling(Map.of("status", 2L, "globalTags", 5L), 1_000L, 2_000L);

    // Equal: a precondition that holds. Lower, higher or absent at capture: it does not.
    assertTrue(ceiling.callerMismatches(Map.of("status", 2L)).isEmpty());
    assertEquals(
        ceiling.callerMismatches(Map.of("status", 2L, "globalTags", 4L, "ownership", 1L)),
        Set.of("globalTags", "ownership"));
    assertEquals(ceiling.callerMismatches(Map.of("globalTags", 6L)), Set.of("globalTags"));
  }

  @Test
  public void rejectsAVersionBelowOne() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeleteCeiling(Map.of("status", 0L), 1_000L, 2_000L));
  }

  @Test
  public void legacyThreeArgRollbackRunResultHasNoConditionalOutcome() {
    RollbackRunResult result = new RollbackRunResult(List.of(), 0, List.of());

    assertNull(result.getConditionalDeleteOutcome());
  }
}
