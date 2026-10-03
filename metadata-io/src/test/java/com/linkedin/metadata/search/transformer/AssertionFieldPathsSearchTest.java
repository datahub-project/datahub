package com.linkedin.metadata.search.transformer;

import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionStdOperator;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.Test;

public class AssertionFieldPathsSearchTest {
  private static final OperationContext CONTEXT =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private static final AspectSpec SPEC =
      CONTEXT
          .getEntityRegistry()
          .getEntitySpec(ASSERTION_ENTITY_NAME)
          .getAspectSpec(ASSERTION_INFO_ASPECT_NAME);
  private static final Urn ASSERTION = UrnUtils.getUrn("urn:li:assertion:test");
  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.events,PROD)");
  private static final SearchDocumentTransformer TRANSFORMER =
      new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH);

  @Test
  public void testCustomColumnSearchProjectionAndRemoval() throws Exception {
    CustomAssertionInfo custom =
        new CustomAssertionInfo()
            .setType("Quality")
            .setEntity(DATASET)
            .setFields(
                new UrnArray(
                    field("col_a"),
                    field("[version=2.0].[type=struct].nested.[type=string].col_b")));
    AssertionInfo info =
        new AssertionInfo().setType(AssertionType.CUSTOM).setCustomAssertion(custom);
    ObjectNode doc = transform(info);
    assertFalse(info.hasFieldPaths());
    assertEquals(doc.path("fieldPath").size(), 2);
    assertEquals(doc.path("fieldPath").get(0).asText(), "col_a");
    assertEquals(
        doc.path("fieldPath").get(1).asText(),
        "[version=2.0].[type=struct].nested.[type=string].col_b");
    custom.setFields(new UrnArray(field("col_c")));
    doc = transform(info);
    assertEquals(doc.path("fieldPath").size(), 1);
    assertEquals(doc.path("fieldPath").get(0).asText(), "col_c");
    custom.setFields(new UrnArray());
    doc = transform(info);
    assertTrue(doc.path("fieldPath").isNull() || doc.path("fieldPath").isEmpty());
  }

  @Test
  public void testLegacyDatasetUsesTheSameSearchField() throws Exception {
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.DATASET)
            .setDatasetAssertion(
                new DatasetAssertionInfo()
                    .setDataset(DATASET)
                    .setScope(DatasetAssertionScope.DATASET_COLUMN)
                    .setOperator(AssertionStdOperator.EQUAL_TO)
                    .setFields(new UrnArray(field("col_a"))));
    assertEquals(transform(info).path("fieldPath").get(0).asText(), "col_a");
  }

  @Test
  public void testNativeFieldPathSurvivesAbsentCustomAndDatasetBranches() throws Exception {
    AssertionInfo info =
        new AssertionInfo()
            .setType(AssertionType.FIELD)
            .setFieldAssertion(
                new FieldAssertionInfo()
                    .setType(FieldAssertionType.FIELD_VALUES)
                    .setEntity(DATASET)
                    .setFieldPath("col_a"));
    assertEquals(transform(info).path("fieldPath").get(0).asText(), "col_a");
  }

  private static Urn field(String path) {
    return Urn.createFromTuple("schemaField", DATASET.toString(), path);
  }

  private static ObjectNode transform(AssertionInfo info) throws Exception {
    return TRANSFORMER
        .transformAspect(
            CONTEXT, ASSERTION, info, SPEC, false, AuditStampUtils.createDefaultAuditStamp())
        .orElseThrow();
  }
}
