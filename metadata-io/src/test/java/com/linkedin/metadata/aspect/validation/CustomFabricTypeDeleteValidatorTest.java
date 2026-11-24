package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DEPRECATION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ValidationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CustomFabricTypeDeleteValidatorTest {

  private static final Urn TYPE = UrnUtils.getUrn("urn:li:customFabricType:MY_ENV");
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.tbl,MY_ENV)");

  private CustomFabricTypeDeleteValidator validator;
  private SearchRetriever searchRetriever;
  private AspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setUp() {
    validator =
        new CustomFabricTypeDeleteValidator()
            .setFields(FabricTypeNameFields.discover(TestOperationContexts.defaultEntityRegistry()))
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
    when(searchRetriever.scroll(anyList(), any(), any(), anyInt(), anyList(), any()))
        .thenReturn(result());
  }

  @Test
  public void rejectsHardDeleteWhileAnEntityUsesTheType() {
    whenSearching("origin", DATASET);

    List<AspectValidationException> errors =
        validateDelete(CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME, OperationFingerprint.EMPTY);
    assertEquals(errors.size(), 1);
    assertTrue(errors.get(0).getMessage().contains(DATASET.toString()));
  }

  @Test
  public void rejectsSoftDeleteWhileAnEntityUsesTheType() {
    whenSearching("env", UrnUtils.getUrn("urn:li:dataFlow:(airflow,flow,MY_ENV)"));

    assertEquals(validateStatus(removed(true), null, OperationFingerprint.EMPTY).size(), 1);
  }

  /** One search covers every indexed environment field, over every entity type that has one. */
  @Test
  public void checksEveryIndexedFieldInOneSearch() {
    validateDelete(CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME, OperationFingerprint.EMPTY);

    ArgumentCaptor<List<String>> entities = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<Filter> filter = ArgumentCaptor.forClass(Filter.class);
    verify(searchRetriever, times(1))
        .scroll(entities.capture(), filter.capture(), any(), anyInt(), anyList(), any());
    assertEquals(
        new TreeSet<>(entities.getValue()),
        new TreeSet<>(
            List.of(
                "container",
                "dataFlow",
                "dataJob",
                "dataProcess",
                "dataset",
                "mlModel",
                "mlModelDeployment",
                "mlModelGroup")));
    assertEquals(filter.getValue().getOr().size(), 2);
    assertTrue(filter.getValue().toString().contains("origin"));
    assertTrue(filter.getValue().toString().contains("env"));
  }

  @Test
  public void allowsDeletingAnUnusedType() {
    assertTrue(
        validateDelete(CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME, OperationFingerprint.EMPTY).isEmpty());
    assertTrue(validateStatus(removed(true), null, OperationFingerprint.EMPTY).isEmpty());
  }

  @Test
  public void deprecationAndOtherWritesAreNeverBlocked() {
    whenSearching("origin", DATASET);

    assertTrue(validateDelete(DEPRECATION_ASPECT_NAME, OperationFingerprint.EMPTY).isEmpty());
    assertTrue(validateStatus(removed(false), null, OperationFingerprint.EMPTY).isEmpty());
    assertTrue(validateStatus(removed(true), removed(true), OperationFingerprint.EMPTY).isEmpty());
    verify(searchRetriever, never()).scroll(anyList(), any(), any(), anyInt(), anyList(), any());
  }

  @Test
  public void allowsRollbackDeletes() {
    whenSearching("origin", DATASET);
    OperationContext rollback = mock(OperationContext.class);
    when(rollback.getValidationContext())
        .thenReturn(ValidationContext.builder().isRollbackDeletion(true).build());

    assertTrue(validateDelete(CUSTOM_FABRIC_TYPE_KEY_ASPECT_NAME, rollback).isEmpty());
    assertTrue(validateStatus(removed(true), null, rollback).isEmpty());
  }

  private void whenSearching(String field, Urn... users) {
    when(searchRetriever.scroll(
            anyList(),
            argThat((Filter filter) -> filter != null && filter.toString().contains(field)),
            any(),
            anyInt(),
            anyList(),
            notNull()))
        .thenReturn(result(users));
  }

  private List<AspectValidationException> validateDelete(
      String aspectName, OperationFingerprint operationContext) {
    BatchItem item = mock(BatchItem.class);
    when(item.getUrn()).thenReturn(TYPE);
    when(item.getChangeType()).thenReturn(ChangeType.DELETE);
    when(item.getAspectName()).thenReturn(aspectName);
    return validator
        .validateProposedAspects(operationContext, List.of(item), retrieverContext)
        .toList();
  }

  private List<AspectValidationException> validateStatus(
      Status proposed, @Nullable Status previous, OperationFingerprint operationContext) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(TYPE);
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

  private static ScrollResult result(Urn... urns) {
    SearchEntityArray entities = new SearchEntityArray();
    for (Urn urn : urns) {
      entities.add(new SearchEntity().setEntity(urn));
    }
    return new ScrollResult().setEntities(entities).setNumEntities(urns.length);
  }

  private static Status removed(boolean removed) {
    return new Status().setRemoved(removed);
  }
}
