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
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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

  @Test
  public void rejectsChainWhenSameBatchParentIsRejected() {
    // The MCE consumer would commit grandchild and child even though parent never exists.
    Urn grandchildUrn = UrnUtils.getUrn("urn:li:dataProduct:grandchild");
    mockExists(PARENT_URN, false);
    ChangeMCP child = item(CHILD_URN, properties(PARENT_URN), null);
    ChangeMCP grandchild = item(grandchildUrn, properties(CHILD_URN), null);

    List<AspectValidationException> errors =
        validator
            .validatePreCommitAspects(
                OperationFingerprint.EMPTY, List.of(grandchild, child), retrieverContext)
            .toList();

    assertEquals(
        errors.stream().map(AspectValidationException::getEntityUrn).collect(Collectors.toSet()),
        Set.of(CHILD_URN, grandchildUrn));
  }

  @Test
  public void rejectsParentThatIsADescendant() {
    // other -> parent -> child, so making other the parent of child closes a loop.
    mockExists(OTHER_PARENT_URN, true);
    mockStoredParents(Map.of(OTHER_PARENT_URN, PARENT_URN, PARENT_URN, CHILD_URN));

    assertEquals(validate(item(CHILD_URN, properties(OTHER_PARENT_URN), null)).size(), 1);
  }

  @Test
  public void allowsParentWithUnrelatedAncestors() {
    Urn rootUrn = UrnUtils.getUrn("urn:li:dataProduct:root");
    mockExists(OTHER_PARENT_URN, true);
    mockStoredParents(Map.of(OTHER_PARENT_URN, PARENT_URN, PARENT_URN, rootUrn));

    assertTrue(validate(item(CHILD_URN, properties(OTHER_PARENT_URN), null)).isEmpty());
  }

  @Test
  public void stopsAtStoredCycleThatExcludesTheProduct() {
    mockExists(OTHER_PARENT_URN, true);
    mockStoredParents(Map.of(OTHER_PARENT_URN, PARENT_URN, PARENT_URN, OTHER_PARENT_URN));

    assertTrue(validate(item(CHILD_URN, properties(OTHER_PARENT_URN), null)).isEmpty());
  }

  @Test
  public void rejectsCycleWrittenInOneBatch() {
    ChangeMCP child = item(CHILD_URN, properties(PARENT_URN), null);
    ChangeMCP parent = item(PARENT_URN, properties(CHILD_URN), null);

    List<AspectValidationException> errors =
        validator
            .validatePreCommitAspects(
                OperationFingerprint.EMPTY, List.of(child, parent), retrieverContext)
            .toList();

    assertEquals(
        errors.stream().map(AspectValidationException::getEntityUrn).collect(Collectors.toSet()),
        Set.of(CHILD_URN, PARENT_URN));
  }

  @Test
  public void allowsMoveWhenSameBatchDetachesTheDescendant() {
    // parent is stored under child; detaching it in the same batch lets child move under it.
    mockStoredParents(Map.of(PARENT_URN, CHILD_URN));
    ChangeMCP detached =
        item(PARENT_URN, new DataProductProperties().setName("parent"), properties(CHILD_URN));
    ChangeMCP moved = item(CHILD_URN, properties(PARENT_URN), null);

    assertTrue(
        validator
            .validatePreCommitAspects(
                OperationFingerprint.EMPTY, List.of(detached, moved), retrieverContext)
            .toList()
            .isEmpty());
  }

  @Test
  public void walksPastRejectedItemToItsStoredParent() {
    // Stored: d -> b -> a. b's move to a missing parent is rejected, so b stays under a and
    // moving a under d would close the loop a -> d -> b -> a.
    Urn a = UrnUtils.getUrn("urn:li:dataProduct:a");
    Urn b = UrnUtils.getUrn("urn:li:dataProduct:b");
    Urn d = UrnUtils.getUrn("urn:li:dataProduct:d");
    Urn missing = UrnUtils.getUrn("urn:li:dataProduct:missing");
    when(aspectRetriever.entityExists(any(), eq(Set.of(d, missing))))
        .thenReturn(Map.of(d, true, missing, false));
    mockStoredParents(Map.of(d, b, b, a));
    ChangeMCP moveA = item(a, properties(d), new DataProductProperties().setName("a"));
    ChangeMCP moveB = item(b, properties(missing), properties(a));

    assertEquals(rejectedUrns(moveA, moveB), Set.of(a, b));
  }

  @Test
  public void rewalksAfterACycleRejection() {
    // Stored: c -> b -> a. b's move under c is a cycle, so b stays under a. The first walk for a
    // saw b's proposed parent; only the second sees that a -> c -> b -> a closes a loop.
    Urn a = UrnUtils.getUrn("urn:li:dataProduct:a");
    Urn b = UrnUtils.getUrn("urn:li:dataProduct:b");
    Urn c = UrnUtils.getUrn("urn:li:dataProduct:c");
    when(aspectRetriever.entityExists(any(), eq(Set.of(c)))).thenReturn(Map.of(c, true));
    mockStoredParents(Map.of(c, b, b, a));
    ChangeMCP moveB = item(b, properties(c), properties(a));
    ChangeMCP moveA = item(a, properties(c), new DataProductProperties().setName("a"));

    assertEquals(rejectedUrns(moveB, moveA), Set.of(a, b));
  }

  private Set<Urn> rejectedUrns(ChangeMCP... items) {
    return validator
        .validatePreCommitAspects(OperationFingerprint.EMPTY, List.of(items), retrieverContext)
        .map(AspectValidationException::getEntityUrn)
        .collect(Collectors.toSet());
  }

  private List<AspectValidationException> validate(ChangeMCP item) {
    return validator
        .validatePreCommitAspects(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
        .toList();
  }

  private void mockExists(Urn urn, boolean exists) {
    when(aspectRetriever.entityExists(any(), eq(Set.of(urn)))).thenReturn(Map.of(urn, exists));
  }

  private void mockStoredParents(Map<Urn, Urn> parents) {
    when(aspectRetriever.getLatestAspectObjects(any(), anySet(), anySet()))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                if (parents.containsKey(urn)) {
                  result.put(
                      urn,
                      Map.of(
                          "dataProductProperties",
                          new Aspect(properties(parents.get(urn)).data())));
                }
              }
              return result;
            });
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
