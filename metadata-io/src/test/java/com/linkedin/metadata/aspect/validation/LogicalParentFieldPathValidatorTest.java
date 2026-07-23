package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.logical.LogicalParent;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
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

  private LogicalParentFieldPathValidator validator;
  private CachingAspectRetriever mockAspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    validator =
        new LogicalParentFieldPathValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(List.of())
                    .build());
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

  private BatchItem columnLink(String childField, String parentField) {
    Urn childFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(CHILD_DATASET, childField);
    Urn parentFieldUrn = SchemaFieldUtils.generateSchemaFieldUrn(PARENT_DATASET, parentField);
    LogicalParent aspect =
        new LogicalParent().setParent(new Edge().setDestinationUrn(parentFieldUrn));
    return TestMCP.ofOneUpsertItem(childFieldUrn, aspect, registry).stream().findFirst().get();
  }

  @Test
  public void testValidWhenBothFieldPathsExist() {
    stubSchema(CHILD_DATASET, "id");
    stubSchema(PARENT_DATASET, "id");

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
                OperationFingerprint.EMPTY, List.of(columnLink("id", "id")), retrieverContext)
            .toList();

    Assert.assertTrue(exceptions.isEmpty());
  }

  @Test
  public void testRejectsWhenParentFieldMissing() {
    stubSchema(CHILD_DATASET, "id");
    stubSchema(PARENT_DATASET, "other");

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
                OperationFingerprint.EMPTY, List.of(columnLink("id", "id")), retrieverContext)
            .toList();

    Assert.assertFalse(exceptions.isEmpty());
  }

  @Test
  public void testRejectsWhenChildFieldMissing() {
    stubSchema(CHILD_DATASET, "other");
    stubSchema(PARENT_DATASET, "id");

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(
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
    BatchItem item =
        TestMCP.ofOneUpsertItem(childFieldUrn, unlink, registry).stream().findFirst().get();

    List<AspectValidationException> exceptions =
        validator
            .validateProposedAspects(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
            .toList();

    Assert.assertTrue(exceptions.isEmpty());
    Mockito.verifyNoInteractions(mockAspectRetriever);
  }
}
