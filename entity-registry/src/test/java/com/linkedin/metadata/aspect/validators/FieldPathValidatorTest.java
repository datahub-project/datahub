package com.linkedin.metadata.aspect.validators;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.validation.CreateIfNotExistsValidator;
import com.linkedin.metadata.aspect.validation.FieldPathValidator;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
  private DatasetUrn testDatasetUrn;
  private final FieldPathValidator test = new FieldPathValidator(validatorConfig);

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
        test.validateProposed(
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
            .count(),
        0);
  }

  @Test
  public void testValidateNonDuplicatedSchemaFieldPath() throws URISyntaxException {
    final SchemaMetadata schema = getMockSchemaMetadataAspect(false);
    assertEquals(
        test.validateProposed(
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
            .count(),
        0);
  }

  @Test
  public void testValidateDuplicatedSchemaFieldPath() throws URISyntaxException {
    final SchemaMetadata schema = getMockSchemaMetadataAspect(true);

    assertEquals(
        test.validateProposed(
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
            .count(),
        1);
  }

  @Test
  public void testValidateDeleteDuplicatedSchemaFieldPath() throws URISyntaxException {
    final SchemaMetadata schema = getMockSchemaMetadataAspect(true);

    assertEquals(
        test.validateProposed(
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
            .count(),
        0);
  }

  private SchemaMetadata getMockSchemaMetadataAspect(boolean duplicateFields)
      throws URISyntaxException {

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
}
