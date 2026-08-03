package com.linkedin.metadata.entity.coordinator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.test.metadata.aspect.batch.TestSystemAspect;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class MutationOverlayTest {

  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,foo,PROD)");
  private static final String ASPECT = "status";

  private static final OperationFingerprint CONTEXT = mock(OperationFingerprint.class);

  private static ChangeMCP upsertMcp(boolean removed) {
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn(ASPECT);
    EntitySpec entitySpec = mock(EntitySpec.class);
    return TestMCP.builder()
        .urn(URN)
        .changeType(ChangeType.UPSERT)
        .entitySpec(entitySpec)
        .aspectSpec(aspectSpec)
        .recordTemplate(new Status().setRemoved(removed))
        .build();
  }

  @Test
  public void proposedUpsertShadowsDatabaseRead() {
    AspectRetriever delegate = mock(AspectRetriever.class);
    when(delegate.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(Map.of(URN, Map.of(ASPECT, new Aspect(new Status().setRemoved(true).data()))));

    MutationOverlay overlay = MutationOverlay.of(delegate).put(PlannedUpsert.of(upsertMcp(false)));

    Aspect result =
        overlay.getLatestAspectObjects(CONTEXT, Set.of(URN), Set.of(ASPECT)).get(URN).get(ASPECT);

    // Overlay's proposed (removed=false) wins over the database (removed=true).
    assertFalse(new Status(result.data()).isRemoved());
  }

  @Test
  public void proposedUpsertShadowsSystemAspectRead() {
    AspectRetriever delegate = mock(AspectRetriever.class);
    SystemAspect dbAspect =
        TestSystemAspect.builder().urn(URN).recordTemplate(new Status().setRemoved(true)).build();
    when(delegate.getLatestSystemAspects(any(), any()))
        .thenReturn(Map.of(URN, Map.of(ASPECT, dbAspect)));

    MutationOverlay overlay = MutationOverlay.of(delegate).put(PlannedUpsert.of(upsertMcp(false)));

    SystemAspect result =
        overlay.getLatestSystemAspects(CONTEXT, Map.of(URN, Set.of(ASPECT))).get(URN).get(ASPECT);

    assertFalse(((Status) result.getAspect(Status.class)).isRemoved());
  }

  @Test
  public void plannedDeleteHidesDatabaseRead() {
    AspectRetriever delegate = mock(AspectRetriever.class);
    when(delegate.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(Map.of(URN, Map.of(ASPECT, new Aspect(new Status().data()))));

    MutationOverlay overlay =
        MutationOverlay.of(delegate).put(PlannedDelete.of(URN.toString(), ASPECT));

    Map<Urn, Map<String, Aspect>> result =
        overlay.getLatestAspectObjects(CONTEXT, Set.of(URN), Set.of(ASPECT));

    // The deleted aspect is hidden; with no other aspects the urn drops out entirely.
    assertTrue(result.isEmpty());
  }

  @Test
  public void unaffectedReadFallsThroughToDelegate() {
    AspectRetriever delegate = mock(AspectRetriever.class);
    Aspect dbAspect = new Aspect(new Status().setRemoved(true).data());
    when(delegate.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(Map.of(URN, Map.of(ASPECT, dbAspect)));

    // Overlay has a mutation for a different aspect only; the requested aspect is untouched.
    MutationOverlay overlay =
        MutationOverlay.of(delegate).put(PlannedDelete.of(URN.toString(), "datasetProperties"));

    Aspect result =
        overlay.getLatestAspectObjects(CONTEXT, Set.of(URN), Set.of(ASPECT)).get(URN).get(ASPECT);

    assertTrue(new Status(result.data()).isRemoved());
  }

  @Test
  public void proposedUpsertMakesEntityExist() {
    AspectRetriever delegate = mock(AspectRetriever.class);
    when(delegate.entityExists(any(), any())).thenReturn(Map.of(URN, false));

    MutationOverlay overlay = MutationOverlay.of(delegate).put(PlannedUpsert.of(upsertMcp(false)));

    assertTrue(overlay.entityExists(CONTEXT, Set.of(URN)).get(URN));
  }

  @Test
  public void putReturnsNewOverlayLeavingOriginalUnchanged() {
    AspectRetriever delegate = mock(AspectRetriever.class);
    when(delegate.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(Map.of(URN, Map.of(ASPECT, new Aspect(new Status().setRemoved(true).data()))));

    MutationOverlay empty = MutationOverlay.of(delegate);
    MutationOverlay withUpsert = empty.put(PlannedUpsert.of(upsertMcp(false)));

    // Original overlay still reflects the database read.
    assertTrue(
        new Status(
                empty
                    .getLatestAspectObjects(CONTEXT, Set.of(URN), Set.of(ASPECT))
                    .get(URN)
                    .get(ASPECT)
                    .data())
            .isRemoved());
    // New overlay reflects the proposed change.
    assertFalse(
        new Status(
                withUpsert
                    .getLatestAspectObjects(CONTEXT, Set.of(URN), Set.of(ASPECT))
                    .get(URN)
                    .get(ASPECT)
                    .data())
            .isRemoved());
  }

  @Test
  public void getEntityRegistryDelegates() {
    AspectRetriever delegate = mock(AspectRetriever.class);
    when(delegate.getEntityRegistry()).thenReturn(null);
    assertEquals(MutationOverlay.of(delegate).getEntityRegistry(), null);
  }
}
