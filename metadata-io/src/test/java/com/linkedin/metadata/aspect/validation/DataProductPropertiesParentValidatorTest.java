package com.linkedin.metadata.aspect.validation;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProductPropertiesParentValidatorTest {

  private static final Urn CHILD_URN = UrnUtils.getUrn("urn:li:dataProduct:child");
  private static final Urn PARENT_URN = UrnUtils.getUrn("urn:li:dataProduct:parent");
  private static final Urn OTHER_PARENT_URN = UrnUtils.getUrn("urn:li:dataProduct:other");

  private DataProductPropertiesParentValidator validator;
  private AspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setUp() {
    validator =
        new DataProductPropertiesParentValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(List.of())
                    .build());
    aspectRetriever = mock(AspectRetriever.class);
    retrieverContext = mock(RetrieverContext.class);
    when(retrieverContext.getAspectRetriever()).thenReturn(aspectRetriever);
  }

  @Test
  public void allowsExistingParent() {
    mockExists(PARENT_URN, true);

    assertTrue(validate(item(CHILD_URN, properties(PARENT_URN), null)).isEmpty());
  }

  @Test
  public void rejectsMissingParent() {
    mockExists(PARENT_URN, false);

    assertEquals(validate(item(CHILD_URN, properties(PARENT_URN), null)).size(), 1);
  }

  @Test
  public void rejectsNonDataProductParent() {
    Urn domain = UrnUtils.getUrn("urn:li:domain:finance");

    assertEquals(validate(item(CHILD_URN, properties(domain), null)).size(), 1);
    verify(aspectRetriever, never()).entityExists(any(), anySet());
  }

  @Test
  public void rejectsSelfParent() {
    assertEquals(validate(item(CHILD_URN, properties(CHILD_URN), null)).size(), 1);
  }

  @Test
  public void allowsUnchangedParentWithoutLookup() {
    // The stored parent may since have been deleted; renaming the child must still work.
    ChangeMCP renamed =
        item(
            CHILD_URN,
            properties(PARENT_URN).setName("renamed"),
            properties(PARENT_URN).setName("child"));

    assertTrue(validate(renamed).isEmpty());
    verify(aspectRetriever, never()).entityExists(any(), anySet());
  }

  @Test
  public void checksReplacedParent() {
    mockExists(OTHER_PARENT_URN, false);

    assertEquals(
        validate(item(CHILD_URN, properties(OTHER_PARENT_URN), properties(PARENT_URN))).size(), 1);
  }

  @Test
  public void allowsParentCreatedInSameBatch() {
    ChangeMCP parent = item(PARENT_URN, new DataProductProperties().setName("parent"), null);
    ChangeMCP child = item(CHILD_URN, properties(PARENT_URN), null);

    assertTrue(
        validator
            .validatePreCommitAspects(
                OperationFingerprint.EMPTY, List.of(parent, child), retrieverContext)
            .toList()
            .isEmpty());
    verify(aspectRetriever, never()).entityExists(any(), anySet());
  }

  private List<AspectValidationException> validate(ChangeMCP item) {
    return validator
        .validatePreCommitAspects(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
        .toList();
  }

  private void mockExists(Urn urn, boolean exists) {
    when(aspectRetriever.entityExists(any(), eq(Set.of(urn)))).thenReturn(Map.of(urn, exists));
  }

  private static ChangeMCP item(
      Urn urn, DataProductProperties proposed, @Nullable DataProductProperties previous) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(urn);
    when(item.getAspect(DataProductProperties.class)).thenReturn(proposed);
    if (previous != null) {
      when(item.getPreviousRecordTemplate()).thenReturn(previous);
      when(item.getPreviousAspect(DataProductProperties.class)).thenReturn(previous);
    }
    return item;
  }

  private static DataProductProperties properties(Urn parent) {
    return new DataProductProperties().setName("child").setParentDataProduct(parent);
  }
}
