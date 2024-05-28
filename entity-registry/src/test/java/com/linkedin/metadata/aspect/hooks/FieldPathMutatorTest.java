package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.EDITABLE_SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
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
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class FieldPathMutatorTest {

  private EntityRegistry entityRegistry;
  private RetrieverContext mockRetrieverContext;
  private DatasetUrn testDatasetUrn;
  private final FieldPathMutator test =
      new FieldPathMutator().setConfig(mock(AspectPluginConfig.class));

  @BeforeTest
  public void init() throws URISyntaxException {
    testDatasetUrn =
        DatasetUrn.createFromUrn(
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)"));

    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testValidateIncorrectAspect() {
    final Domains domains =
        new Domains()
            .setDomains(new UrnArray(ImmutableList.of(UrnUtils.getUrn("urn:li:domain:123"))));
    assertEquals(
        test.writeMutation(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testDatasetUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testDatasetUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testDatasetUrn.getEntityType())
                                .getAspectSpec(DOMAINS_ASPECT_NAME))
                        .recordTemplate(domains)
                        .build()),
                mockRetrieverContext)
            .filter(Pair::getSecond)
            .count(),
        0);
  }

  @Test
  public void testValidateNonDuplicatedSchemaFieldPath() {
    final SchemaMetadata schema = getMockSchemaMetadataAspect(false);
    assertEquals(
        test.writeMutation(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testDatasetUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testDatasetUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testDatasetUrn.getEntityType())
                                .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
                        .recordTemplate(schema)
                        .build()),
                mockRetrieverContext)
            .filter(Pair::getSecond)
            .count(),
        0);
  }

  @Test
  public void testValidateDuplicatedSchemaFieldPath() {
    final SchemaMetadata schema = getMockSchemaMetadataAspect(true);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testDatasetUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testDatasetUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testDatasetUrn.getEntityType())
                                .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
                        .recordTemplate(schema)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    assertEquals(result.get(0).getFirst().getAspect(SchemaMetadata.class).getFields().size(), 1);
  }

  @Test
  public void testValidateDeleteDuplicatedSchemaFieldPath() {
    final SchemaMetadata schema = getMockSchemaMetadataAspect(true);

    assertEquals(
        test.writeMutation(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.DELETE)
                        .urn(testDatasetUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testDatasetUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testDatasetUrn.getEntityType())
                                .getAspectSpec(SCHEMA_METADATA_ASPECT_NAME))
                        .recordTemplate(schema)
                        .build()),
                mockRetrieverContext)
            .filter(Pair::getSecond)
            .count(),
        0);
  }

  @Test
  public void testValidateNonDuplicatedEditableSchemaFieldPath() {
    final EditableSchemaMetadata schema = getMockEditableSchemaMetadataAspect(false);
    assertEquals(
        test.writeMutation(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testDatasetUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testDatasetUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testDatasetUrn.getEntityType())
                                .getAspectSpec(EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
                        .recordTemplate(schema)
                        .build()),
                mockRetrieverContext)
            .filter(Pair::getSecond)
            .count(),
        0);
  }

  @Test
  public void testValidateDuplicatedEditableSchemaFieldPath() {
    final EditableSchemaMetadata schema = getMockEditableSchemaMetadataAspect(true);

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(
                Set.of(
                    TestMCP.builder()
                        .changeType(ChangeType.UPSERT)
                        .urn(testDatasetUrn)
                        .entitySpec(entityRegistry.getEntitySpec(testDatasetUrn.getEntityType()))
                        .aspectSpec(
                            entityRegistry
                                .getEntitySpec(testDatasetUrn.getEntityType())
                                .getAspectSpec(EDITABLE_SCHEMA_METADATA_ASPECT_NAME))
                        .recordTemplate(schema)
                        .build()),
                mockRetrieverContext)
            .collect(Collectors.toList());

    assertEquals(result.stream().filter(Pair::getSecond).count(), 1);
    assertEquals(
        result
            .get(0)
            .getFirst()
            .getAspect(EditableSchemaMetadata.class)
            .getEditableSchemaFieldInfo()
            .size(),
        1);
  }

  private SchemaMetadata getMockSchemaMetadataAspect(boolean duplicateFields) {
    List<SchemaField> fields = new ArrayList<>();
    fields.add(
        new SchemaField()
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNullable(false)
            .setNativeDataType("string")
            .setFieldPath("test"));

    if (duplicateFields) {
      fields.add(
          new SchemaField()
              .setType(
                  new SchemaFieldDataType()
                      .setType(SchemaFieldDataType.Type.create(new StringType())))
              .setNullable(false)
              .setNativeDataType("string")
              .setFieldPath("test"));
    }

    return new SchemaMetadata()
        .setPlatform(testDatasetUrn.getPlatformEntity())
        .setFields(new SchemaFieldArray(fields));
  }

  private EditableSchemaMetadata getMockEditableSchemaMetadataAspect(boolean duplicateFields) {

    List<EditableSchemaFieldInfo> fields = new ArrayList<>();
    fields.add(new EditableSchemaFieldInfo().setFieldPath("test"));

    if (duplicateFields) {
      fields.add(new EditableSchemaFieldInfo().setFieldPath("test"));
    }

    return new EditableSchemaMetadata()
        .setEditableSchemaFieldInfo(new EditableSchemaFieldInfoArray(fields));
  }
}
