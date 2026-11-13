package com.linkedin.metadata.entity.logical;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class LogicalModelUtilsTest {

  private static final Urn PARENT =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:logical,p,PROD)");
  private static final Urn CHILD =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,c,PROD)");

  private static final Urn OTHER_PARENT =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:logical,other,PROD)");

  @Test
  public void testBuildUnlinkClearsOnlyLinksToTheCurrentParent() {
    // Child is linked to PARENT at dataset level and on columns "id" and "name"; column "code" maps
    // to a different logical model; column "unlinked" has no logical parent at all.
    Map<Urn, Urn> currentParents = new HashMap<>();
    currentParents.put(CHILD, PARENT);
    currentParents.put(field(CHILD, "id"), field(PARENT, "id"));
    currentParents.put(field(CHILD, "name"), field(PARENT, "name"));
    currentParents.put(field(CHILD, "code"), field(OTHER_PARENT, "code"));
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(retrieverOf(currentParents));

    List<MetadataChangeProposal> proposals =
        LogicalModelUtils.buildUnlinkProposals(
            CHILD, List.of("id", "name", "code", "unlinked"), opContext);

    assertEquals(
        proposals.stream().map(MetadataChangeProposal::getEntityUrn).collect(Collectors.toSet()),
        Set.of(CHILD, field(CHILD, "id"), field(CHILD, "name")),
        "only the dataset link and the columns mapped to the unlinked parent are cleared");
  }

  @Test
  public void testBuildUnlinkWithoutDatasetLevelParentClearsAllColumnLinks() {
    // No dataset-level parent identifies a model to scope to, so every existing column link goes.
    Map<Urn, Urn> currentParents = new HashMap<>();
    currentParents.put(field(CHILD, "id"), field(PARENT, "id"));
    currentParents.put(field(CHILD, "code"), field(OTHER_PARENT, "code"));
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(retrieverOf(currentParents));

    List<MetadataChangeProposal> proposals =
        LogicalModelUtils.buildUnlinkProposals(CHILD, List.of("id", "code", "unlinked"), opContext);

    assertEquals(
        proposals.stream().map(MetadataChangeProposal::getEntityUrn).collect(Collectors.toSet()),
        Set.of(CHILD, field(CHILD, "id"), field(CHILD, "code")));
  }

  private static Urn field(Urn dataset, String fieldPath) {
    return SchemaFieldUtils.generateSchemaFieldUrn(dataset, fieldPath);
  }

  /** An AspectRetriever serving a logicalParent aspect for each entry of {@code parents}. */
  @SuppressWarnings("unchecked")
  private static AspectRetriever retrieverOf(Map<Urn, Urn> parents) {
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getLatestAspectObjects(any(), anySet(), anySet()))
        .thenAnswer(
            invocation -> {
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : (Set<Urn>) invocation.getArgument(1)) {
                if (parents.containsKey(urn)) {
                  result.put(
                      urn,
                      Map.of(
                          LOGICAL_PARENT_ASPECT_NAME,
                          new Aspect(
                              LogicalModelUtils.createLogicalParent(
                                      parents.get(urn),
                                      TestOperationContexts.systemContextNoSearchAuthorization())
                                  .data())));
                }
              }
              return result;
            });
    return retriever;
  }

  @Test
  public void testBuildLinkProposalsCreatesDatasetAndColumnEdges() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    List<MetadataChangeProposal> proposals =
        LogicalModelUtils.buildLinkProposals(CHILD, PARENT, Map.of("id", "ID"), opContext);
    // 1 dataset-level + 1 column-level
    assertEquals(proposals.size(), 2);
  }

  @Test
  public void testBuildPartialUnlinkClearsOnlyGivenColumnsWhenDatasetKept() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    List<MetadataChangeProposal> proposals =
        LogicalModelUtils.buildPartialUnlinkProposals(CHILD, List.of("x"), false, opContext);
    // Only the single column edge is cleared; no dataset-level proposal is emitted.
    assertEquals(proposals.size(), 1);
    assertFalse(
        proposals.stream().anyMatch(p -> DATASET_ENTITY_NAME.equals(p.getEntityType())),
        "dataset-level link must be preserved");
  }

  @Test
  public void testBuildPartialUnlinkClearsDatasetLevelWhenRequested() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    List<MetadataChangeProposal> proposals =
        LogicalModelUtils.buildPartialUnlinkProposals(CHILD, List.of("x"), true, opContext);
    // Dataset-level + one column-level.
    assertEquals(proposals.size(), 2);
    assertTrue(
        proposals.stream()
            .anyMatch(
                p ->
                    CHILD.equals(p.getEntityUrn())
                        && DATASET_ENTITY_NAME.equals(p.getEntityType())),
        "dataset-level link must be cleared");
  }

  @Test
  public void testValidateFieldPathsRejectsMissingParentField() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LogicalModelUtils.validateFieldPaths(
                java.util.Set.of("id"), java.util.Set.of("ID"), Map.of("missing", "ID")));
  }
}
