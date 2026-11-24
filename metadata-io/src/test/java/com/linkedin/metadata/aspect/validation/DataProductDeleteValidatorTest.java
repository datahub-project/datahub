package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ValidationContext;
import java.util.List;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProductDeleteValidatorTest {

  private static final Urn PARENT_URN = UrnUtils.getUrn("urn:li:dataProduct:parent");
  private static final Urn CHILD_URN = UrnUtils.getUrn("urn:li:dataProduct:child");
  private static final Urn STALE_CHILD_URN = UrnUtils.getUrn("urn:li:dataProduct:stale_child");
  private static final Urn OTHER_PARENT_URN = UrnUtils.getUrn("urn:li:dataProduct:other_parent");

  private DataProductDeleteValidator validator;
  private SearchRetriever searchRetriever;
  private AspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setUp() {
    validator =
        new DataProductDeleteValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("DELETE", "UPSERT"))
                    .supportedEntityAspectNames(List.of())
                    .build());
    searchRetriever = mock(SearchRetriever.class);
    aspectRetriever = mock(AspectRetriever.class);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getSearchRetriever()).thenReturn(searchRetriever);
    when(retrieverContext.getAspectRetriever()).thenReturn(aspectRetriever);
  }

  @Test
  public void rejectsHardDeleteOfParentWithChildren() {
    mockChildren(CHILD_URN);
    pointsAt(CHILD_URN, PARENT_URN);

    assertEquals(
        validateDelete(DATA_PRODUCT_KEY_ASPECT_NAME, OperationFingerprint.EMPTY).size(), 1);
  }

  @Test
  public void errorNamesParentButNotChildren() {
    mockChildren(CHILD_URN);
    pointsAt(CHILD_URN, PARENT_URN);
    when(aspectRetriever.getLatestAspectObject(
            any(), eq(PARENT_URN), eq(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)))
        .thenReturn(new Aspect(new DataProductProperties().setName("Finance").data()));

    String message =
        validateDelete(DATA_PRODUCT_KEY_ASPECT_NAME, OperationFingerprint.EMPTY)
            .get(0)
            .getMessage();

    assertTrue(message.contains("\"Finance\""));
    assertFalse(message.contains(CHILD_URN.toString()));
  }

  @Test
  public void allowsDeleteOfLegacySelfParentWithoutOtherChildren() {
    mockChildren(PARENT_URN);

    assertTrue(validateDelete(DATA_PRODUCT_KEY_ASPECT_NAME, OperationFingerprint.EMPTY).isEmpty());
  }

  /**
   * Reference cleanup clears the child's pointer, then deletes the parent. Search can still list
   * the child, so the delete must be allowed once the child's own aspect no longer names it.
   */
  @Test
  public void allowsHardDeleteWhenChildPointerWasCleared() {
    mockChildren(CHILD_URN);
    when(aspectRetriever.getLatestAspectObject(
            any(), eq(CHILD_URN), eq(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)))
        .thenReturn(new Aspect(new DataProductProperties().setName("child").data()));

    assertTrue(validateDelete(DATA_PRODUCT_KEY_ASPECT_NAME, OperationFingerprint.EMPTY).isEmpty());
  }

  @Test
  public void allowsHardDeleteWhenChildNowPointsElsewhere() {
    mockChildren(CHILD_URN);
    pointsAt(CHILD_URN, OTHER_PARENT_URN);

    assertTrue(validateDelete(DATA_PRODUCT_KEY_ASPECT_NAME, OperationFingerprint.EMPTY).isEmpty());
  }

  /** Stale hits filling the first page must not hide a live child on the next page. */
  @Test
  public void rejectsHardDeleteWhenLiveChildIsPastTheFirstPage() {
    SearchEntityArray stale = new SearchEntityArray(new SearchEntity().setEntity(STALE_CHILD_URN));
    SearchEntityArray live = new SearchEntityArray(new SearchEntity().setEntity(CHILD_URN));
    when(searchRetriever.scroll(anyList(), any(), isNull(), anyInt(), anyList(), any()))
        .thenReturn(new ScrollResult().setEntities(stale).setScrollId("next").setNumEntities(2));
    when(searchRetriever.scroll(anyList(), any(), eq("next"), anyInt(), anyList(), any()))
        .thenReturn(new ScrollResult().setEntities(live).setNumEntities(2));
    pointsAt(CHILD_URN, PARENT_URN);

    assertEquals(
        validateDelete(DATA_PRODUCT_KEY_ASPECT_NAME, OperationFingerprint.EMPTY).size(), 1);
  }

  /** A stale hit ahead of a real child on the same page must not hide the child. */
  @Test
  public void rejectsHardDeleteWhenRealChildFollowsStaleHit() {
    mockChildren(STALE_CHILD_URN, CHILD_URN);
    pointsAt(CHILD_URN, PARENT_URN);

    assertEquals(
        validateDelete(DATA_PRODUCT_KEY_ASPECT_NAME, OperationFingerprint.EMPTY).size(), 1);
  }

  @Test
  public void allowsHardDeleteWithoutChildren() {
    mockChildren();

    assertTrue(validateDelete(DATA_PRODUCT_KEY_ASPECT_NAME, OperationFingerprint.EMPTY).isEmpty());
  }

  @Test
  public void allowsDeletingNonKeyAspectWithoutLookup() {
    assertTrue(
        validateDelete(DATA_PRODUCT_PROPERTIES_ASPECT_NAME, OperationFingerprint.EMPTY).isEmpty());
    verifyNoChildLookup();
  }

  @Test
  public void rejectsSoftDeleteOfParentWithChildren() {
    mockChildren(CHILD_URN);
    pointsAt(CHILD_URN, PARENT_URN);

    assertEquals(validateStatus(removed(true), null, OperationFingerprint.EMPTY).size(), 1);
  }

  @Test
  public void allowsStatusWritesThatDoNotNewlyRemove() {
    mockChildren(CHILD_URN);

    assertTrue(validateStatus(removed(false), null, OperationFingerprint.EMPTY).isEmpty());
    // Already soft-deleted, for example a parent soft-deleted before this check existed.
    assertTrue(validateStatus(removed(true), removed(true), OperationFingerprint.EMPTY).isEmpty());
    verifyNoChildLookup();
  }

  @Test
  public void allowsRollbackDeletes() {
    mockChildren(CHILD_URN);
    OperationContext rollback = mock(OperationContext.class);
    when(rollback.getValidationContext())
        .thenReturn(ValidationContext.builder().isRollbackDeletion(true).build());

    assertTrue(validateDelete(DATA_PRODUCT_KEY_ASPECT_NAME, rollback).isEmpty());
    assertTrue(validateStatus(removed(true), null, rollback).isEmpty());
    verifyNoChildLookup();
  }

  private List<AspectValidationException> validateDelete(
      String aspectName, OperationFingerprint operationContext) {
    BatchItem item = mock(BatchItem.class);
    when(item.getUrn()).thenReturn(PARENT_URN);
    when(item.getChangeType()).thenReturn(ChangeType.DELETE);
    when(item.getAspectName()).thenReturn(aspectName);
    return validator
        .validateProposedAspects(operationContext, List.of(item), retrieverContext)
        .toList();
  }

  private List<AspectValidationException> validateStatus(
      Status proposed, @Nullable Status previous, OperationFingerprint operationContext) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(PARENT_URN);
    when(item.getAspectName()).thenReturn(STATUS_ASPECT_NAME);
    when(item.getAspect(Status.class)).thenReturn(proposed);
    if (previous != null) {
      when(item.getPreviousRecordTemplate()).thenReturn(previous);
      when(item.getPreviousAspect(Status.class)).thenReturn(previous);
    }
    return validator
        .validatePreCommitAspects(operationContext, List.of(item), retrieverContext)
        .toList();
  }

  private void mockChildren(Urn... children) {
    SearchEntityArray entities = new SearchEntityArray();
    for (Urn child : children) {
      entities.add(new SearchEntity().setEntity(child));
    }
    when(searchRetriever.scroll(anyList(), any(), any(), anyInt(), anyList(), any()))
        .thenReturn(new ScrollResult().setEntities(entities).setNumEntities(children.length));
  }

  private void pointsAt(Urn child, Urn parent) {
    when(aspectRetriever.getLatestAspectObject(
            any(), eq(child), eq(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)))
        .thenReturn(
            new Aspect(
                new DataProductProperties().setName("child").setParentDataProduct(parent).data()));
  }

  private void verifyNoChildLookup() {
    verify(searchRetriever, never()).scroll(anyList(), any(), any(), anyInt(), anyList(), any());
  }

  private static Status removed(boolean removed) {
    return new Status().setRemoved(removed);
  }
}
