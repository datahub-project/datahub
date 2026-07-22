package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.LOGICAL_PARENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LogicalParentFieldPathValidatorTest {

  private static final Urn PARENT_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:logical,model,PROD)");
  private static final Urn CHILD_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:snowflake,db.t,PROD)");

  private final EntityRegistry registry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();

  private CachingAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(Mockito.mock(SearchRetriever.class))
            .graphRetriever(Mockito.mock(GraphRetriever.class))
            .cachingAspectRetriever(mockAspectRetriever)
            .build();
  }

  private void stubSchema(Urn datasetUrn, String... fieldPaths) {
    SchemaFieldArray fields = new SchemaFieldArray();
    for (String path : fieldPaths) {
      fields.add(new SchemaField().setFieldPath(path));
    }
    SchemaMetadata schema = new SchemaMetadata().setFields(fields);
    Mockito.doReturn(new Aspect(schema.data()))
        .when(mockAspectRetriever)
        .getLatestAspectObject(
            any(OperationFingerprint.class), eq(datasetUrn), eq(SCHEMA_METADATA_ASPECT_NAME));
  }

  // A patch-applied logicalParent reaches pre-commit as a merged UPSERT ChangeMCP; the validator
  // now runs there, so tests drive that merged item rather than the (now empty) proposed hook.
  private ChangeMCP columnLink(String childField, String parentField) {
    Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(CHILD_DATASET, childField);
    Urn parentFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(PARENT_DATASET, parentField);
    LogicalParent aspect =
        new LogicalParent().setParent(new Edge().setDestinationUrn(parentFieldUrn));
    ChangeMCP item = TestMCP.ofOneMCP(childFieldUrn, aspect, registry).stream().findFirst().get();
    return ((TestMCP) item).toBuilder().changeType(ChangeType.UPSERT).build();
  }

  @Test
  public void testValidWhenBothFieldPathsExist() {
    stubSchema(CHILD_DATASET, "id");
    stubSchema(PARENT_DATASET, "id");

    List<AspectValidationException> exceptions =
        LogicalParentFieldPathValidator.validateFieldPaths(
                OperationFingerprint.EMPTY, List.of(columnLink("id", "id")), retrieverContext)
            .toList();

    Assert.assertTrue(exceptions.isEmpty());
  }

  @Test
  public void testRejectsWhenParentFieldMissing() {
    stubSchema(CHILD_DATASET, "id");
    stubSchema(PARENT_DATASET, "other");

    List<AspectValidationException> exceptions =
        LogicalParentFieldPathValidator.validateFieldPaths(
                OperationFingerprint.EMPTY, List.of(columnLink("id", "id")), retrieverContext)
            .toList();

    Assert.assertFalse(exceptions.isEmpty());
  }

  @Test
  public void testRejectsWhenChildFieldMissing() {
    stubSchema(CHILD_DATASET, "other");
    stubSchema(PARENT_DATASET, "id");

    List<AspectValidationException> exceptions =
        LogicalParentFieldPathValidator.validateFieldPaths(
                OperationFingerprint.EMPTY, List.of(columnLink("id", "id")), retrieverContext)
            .toList();

    Assert.assertFalse(exceptions.isEmpty());
  }

  @Test
  public void testIgnoresUnlink() {
    // An unlink writes a LogicalParent with no parent edge; it must not be validated (and must not
    // read any schema).
    LogicalParent unlink = new LogicalParent();
    Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(CHILD_DATASET, "id");
    ChangeMCP item = TestMCP.ofOneMCP(childFieldUrn, unlink, registry).stream().findFirst().get();

    List<AspectValidationException> exceptions =
        LogicalParentFieldPathValidator.validateFieldPaths(
                OperationFingerprint.EMPTY, List.of(item), retrieverContext)
            .toList();

    Assert.assertTrue(exceptions.isEmpty());
    Mockito.verifyNoInteractions(mockAspectRetriever);
  }

  @Test
  public void testPreCommitRejectsInvalidFieldPathThroughGating() {
    // Regression guard for the PATCH bypass: the check moved from the proposed hook to pre-commit,
    // so a merged UPSERT (which is what a patch-applied logicalParent becomes) must still be
    // rejected. Drive the production validatePreCommit entry with the bean's real change-type and
    // entity/aspect gating so the wiring — not just the check body — is exercised.
    stubSchema(CHILD_DATASET, "id");
    stubSchema(PARENT_DATASET, "other");

    LogicalParentFieldPathValidator gatedValidator =
        new LogicalParentFieldPathValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(
                        List.of(
                            AspectPluginConfig.EntityAspectName.builder()
                                .entityName(SCHEMA_FIELD_ENTITY_NAME)
                                .aspectName(LOGICAL_PARENT_ASPECT_NAME)
                                .build()))
                    .build());

    List<AspectValidationException> exceptions =
        gatedValidator
            .validatePreCommit(
                OperationFingerprint.EMPTY, List.of(columnLink("id", "id")), retrieverContext)
            .toList();

    Assert.assertFalse(exceptions.isEmpty());
  }
}
