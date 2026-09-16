package com.linkedin.metadata.entity.logical;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class LogicalModelUtilsTest {

  private static final Urn PARENT =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:logical,p,PROD)");
  private static final Urn CHILD =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,c,PROD)");

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
