package com.linkedin.metadata.aspect.validators;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.validation.CreateIfNotExistsValidator;
import com.linkedin.metadata.aspect.validation.FieldPathValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class FieldPathValidatorTest {

  private static final AspectPluginConfig validatorConfig =
      AspectPluginConfig.builder()
          .supportedOperations(
              Arrays.stream(ChangeType.values())
                  .map(Objects::toString)
                  .collect(Collectors.toList()))
          .className(CreateIfNotExistsValidator.class.getName())
          .supportedEntityAspectNames(List.of(AspectPluginConfig.EntityAspectName.ALL))
          .enabled(true)
          .build();
  private EntityRegistry entityRegistry;
  private RetrieverContext mockRetrieverContext;
  private static final DatasetUrn TEST_DATASET_URN;
  private final FieldPathValidator test = new FieldPathValidator().setConfig(validatorConfig);

  static {
    try {
      TEST_DATASET_URN =
          DatasetUrn.createFromUrn(
              UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeTest
  public void init() {
    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testValidateNonDuplicatedSchemaFieldPath() {
    final SchemaMetadata schema = getMockSchemaMetadataAspect(false);
    assertEquals(
        test.validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_DATASET_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_DATASET_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_DATASET_URN.getEntityType())
                                .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
                        .recordTemplate(schema)
                        .build()),
                mockRetrieverContext)
            .count(),
        0);
  }

  @Test
  public void testValidateDuplicatedSchemaFieldPath() {
    final SchemaMetadata schema = getMockSchemaMetadataAspect(true);

    assertEquals(
        test.validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_DATASET_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_DATASET_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_DATASET_URN.getEntityType())
                                .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
                        .recordTemplate(schema)
                        .build()),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testValidateNonDuplicatedEditableSchemaFieldPath() {
    final EditableSchemaMetadata schema = getMockEditableSchemaMetadataAspect(false);
    assertEquals(
        test.validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_DATASET_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_DATASET_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_DATASET_URN.getEntityType())
                                .getAspectSpec(EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
                        .recordTemplate(schema)
                        .build()),
                mockRetrieverContext)
            .count(),
        0);
  }

  @Test
  public void testValidateDuplicatedEditableSchemaFieldPath() {
    final EditableSchemaMetadata schema = getMockEditableSchemaMetadataAspect(true);

    assertEquals(
        test.validateProposed(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(TEST_DATASET_URN)
                        .entitySpec(entityRegistry.getEntitySpec(TEST_DATASET_URN.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(TEST_DATASET_URN.getEntityType())
                                .getAspectSpec(EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
                        .recordTemplate(schema)
                        .build()),
                mockRetrieverContext)
            .count(),
        1);
  }

  @Test
  public void testEmptySchemaFieldPath() {
    final SchemaMetadata schema = getMockSchemaMetadataAspect(false, "");
    TestMCP testItem =
        TestMCP.builder()
            .changeType(ChangeType.UPSERT)
            .urn(TEST_DATASET_URN)
            .entitySpec(entityRegistry.getEntitySpec(TEST_DATASET_URN.getEntityType()))
            .aspectSpec(
                entityRegistry
                    .getEntitySpec(TEST_DATASET_URN.getEntityType())
                    .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
            .recordTemplate(schema)
            .build();

    Set<AspectValidationException> exceptions =
        test.validateProposed(Set.of(testItem), mockRetrieverContext).collect(Collectors.toSet());

    assertEquals(
        exceptions,
        Set.of(
            AspectValidationException.forItem(
                testItem, "SchemaMetadata aspect has empty field path.")));
  }

  private static SchemaMetadata getMockSchemaMetadataAspect(boolean duplicateFields) {
    return getMockSchemaMetadataAspect(duplicateFields, null);
  }

  private static SchemaMetadata getMockSchemaMetadataAspect(
      boolean duplicateFields, @Nullable String fieldPath) {
    List<SchemaField> fields = new ArrayList<>();
    fields.add(
        new SchemaField()
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNullable(false)
            .setNativeDataType("string")
            .setFieldPath(fieldPath == null ? "test" : fieldPath));

    if (duplicateFields) {
      fields.add(
          new SchemaField()
              .setType(
                  new SchemaFieldDataType()
                      .setType(SchemaFieldDataType.Type.create(new StringType())))
              .setNullable(false)
              .setNativeDataType("string")
              .setFieldPath(fieldPath == null ? "test" : fieldPath));
    }

    return new SchemaMetadata()
        .setPlatform(TEST_DATASET_URN.getPlatformEntity())
        .setFields(new SchemaFieldArray(fields));
  }

  private static EditableSchemaMetadata getMockEditableSchemaMetadataAspect(
      boolean duplicateFields) {

    List<EditableSchemaFieldInfo> fields = new ArrayList<>();
    fields.add(new EditableSchemaFieldInfo().setFieldPath("test"));

    if (duplicateFields) {
      fields.add(new EditableSchemaFieldInfo().setFieldPath("test"));
    }

    return new EditableSchemaMetadata()
        .setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray(fields));
  }
}
