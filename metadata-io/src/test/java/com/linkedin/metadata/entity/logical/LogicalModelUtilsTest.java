package com.linkedin.metadata.entity.logical;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.utils.GenericRecordUtils;
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
  public void testBuildUnlinkClearsEveryColumnLinkAndSkipsUnlinkedColumns() {
    // Columns "id" and "name" map to PARENT, "code" was left pointing at another model by an
    // earlier relink, and "unlinked" has no logical parent at all.
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
        Set.of(CHILD, field(CHILD, "id"), field(CHILD, "name"), field(CHILD, "code")),
        "every column link is cleared; the never-linked column gets no write");
  }

  @Test
  public void testRelinkToAnotherModelClearsColumnsLeftOnThePreviousModel() {
    // Child was linked to OTHER_PARENT with columns ID and CODE. It is now linked to PARENT,
    // mapping only ID; CODE must not keep pointing at the previous model.
    Map<Urn, Urn> currentParents = new HashMap<>();
    currentParents.put(CHILD, OTHER_PARENT);
    currentParents.put(field(CHILD, "ID"), field(OTHER_PARENT, "id"));
    currentParents.put(field(CHILD, "CODE"), field(OTHER_PARENT, "code"));
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(retrieverOf(currentParents));

    List<MetadataChangeProposal> proposals =
        LogicalModelUtils.buildLinkProposals(
            CHILD, PARENT, Map.of("id", "ID"), List.of("ID", "CODE", "UNMAPPED"), opContext);

    // A cleared link maps to null, which Collectors.toMap rejects.
    Map<Urn, Urn> written = new HashMap<>();
    proposals.forEach(p -> written.put(p.getEntityUrn(), parentWritten(p)));
    assertEquals(written.keySet(), Set.of(CHILD, field(CHILD, "ID"), field(CHILD, "CODE")));
    assertEquals(written.get(CHILD), PARENT);
    assertEquals(written.get(field(CHILD, "ID")), field(PARENT, "id"));
    assertNull(written.get(field(CHILD, "CODE")), "the stale column link must be cleared");
  }

  @Test
  public void testLinkKeepsUnmappedColumnsAlreadyLinkedToTheSameModel() {
    // A link call that maps only some columns must not undo earlier mappings to the same model.
    Map<Urn, Urn> currentParents = new HashMap<>();
    currentParents.put(CHILD, PARENT);
    currentParents.put(field(CHILD, "NAME"), field(PARENT, "name"));
    OperationContext opContext =
        TestOperationContexts.systemContextNoSearchAuthorization(retrieverOf(currentParents));

    List<MetadataChangeProposal> proposals =
        LogicalModelUtils.buildLinkProposals(
            CHILD, PARENT, Map.of("id", "ID"), List.of("ID", "NAME"), opContext);

    assertEquals(
        proposals.stream().map(MetadataChangeProposal::getEntityUrn).collect(Collectors.toSet()),
        Set.of(CHILD, field(CHILD, "ID")));
  }

  private static Urn parentWritten(MetadataChangeProposal proposal) {
    LogicalParent logicalParent =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            LogicalParent.class);
    return logicalParent.hasParent() ? logicalParent.getParent().getDestinationUrn() : null;
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
        LogicalModelUtils.buildLinkProposals(
            CHILD, PARENT, Map.of("id", "ID"), List.of(), opContext);
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
