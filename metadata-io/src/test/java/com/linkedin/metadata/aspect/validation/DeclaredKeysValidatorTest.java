package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.eq;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DeclaredKey;
import com.linkedin.dataset.DeclaredKeyArray;
import com.linkedin.dataset.DeclaredKeys;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeclaredKeysValidatorTest {

  private static final Urn DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:s3,my_bucket/events,PROD)");

  private final EntityRegistry registry =
      TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry();

  private final DeclaredKeysValidator validator =
      new DeclaredKeysValidator()
          .setConfig(
              AspectPluginConfig.builder()
                  .className("test")
                  .enabled(true)
                  .supportedOperations(List.of("UPSERT"))
                  .supportedEntityAspectNames(List.of())
                  .build());

  private CachingAspectRetriever aspectRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    aspectRetriever = Mockito.mock(CachingAspectRetriever.class);
    Mockito.when(
            aspectRetriever.getLatestAspectObjects(
                any(OperationFingerprint.class),
                anySet(),
                eq(java.util.Set.of(SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(Map.of());
    retrieverContext =
        io.datahubproject.metadata.context.RetrieverContext.builder()
            .searchRetriever(Mockito.mock(SearchRetriever.class))
            .graphRetriever(Mockito.mock(GraphRetriever.class))
            .cachingAspectRetriever(aspectRetriever)
            .build();
  }

  private void stubSchema(String... fieldPaths) {
    SchemaFieldArray fields = new SchemaFieldArray();
    for (String path : fieldPaths) {
      fields.add(new SchemaField().setFieldPath(path));
    }
    Mockito.when(
            aspectRetriever.getLatestAspectObjects(
                any(OperationFingerprint.class),
                anySet(),
                eq(java.util.Set.of(SCHEMA_METADATA_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                DATASET,
                Map.of(
                    SCHEMA_METADATA_ASPECT_NAME,
                    new Aspect(new SchemaMetadata().setFields(fields).data()))));
  }

  private List<AspectValidationException> validate(DeclaredKeys keys) {
    return validator
        .validatePreCommitAspects(
            OperationFingerprint.EMPTY, TestMCP.ofOneMCP(DATASET, keys, registry), retrieverContext)
        .toList();
  }

  private static DeclaredKey businessKey(String name, String... fields) {
    return new DeclaredKey().setName(name).setFields(new StringArray(List.of(fields)));
  }

  @Test
  public void testAcceptsKeysThatExistInSchema() {
    stubSchema("id", "as_of_date", "col_a");
    DeclaredKeys keys =
        new DeclaredKeys()
            .setPrimaryKey(new StringArray(List.of("id")))
            .setBusinessKeys(
                new DeclaredKeyArray(List.of(businessKey("natural", "col_a", "as_of_date"))));

    Assert.assertTrue(validate(keys).isEmpty());
  }

  @Test
  public void testAcceptsAnyFieldPathWhenDatasetHasNoSchema() {
    Assert.assertTrue(
        validate(new DeclaredKeys().setPrimaryKey(new StringArray(List.of("id")))).isEmpty());
  }

  @Test
  public void testRejectsFieldPathsMissingFromSchema() {
    stubSchema("id");
    DeclaredKeys keys =
        new DeclaredKeys()
            .setPrimaryKey(new StringArray(List.of("missing")))
            .setBusinessKeys(new DeclaredKeyArray(List.of(businessKey("natural", "other"))));

    Assert.assertEquals(validate(keys).size(), 2);
  }

  @Test
  public void testRejectsEmptyAndDuplicateKeyDefinitions() {
    Assert.assertEquals(
        validate(new DeclaredKeys().setPrimaryKey(new StringArray(List.of()))).size(),
        1,
        "empty PK");
    Assert.assertEquals(
        validate(new DeclaredKeys().setPrimaryKey(new StringArray(List.of("id", "id")))).size(),
        1,
        "duplicate PK field");
    Assert.assertEquals(
        validate(
                new DeclaredKeys()
                    .setBusinessKeys(
                        new DeclaredKeyArray(
                            List.of(businessKey("k", "a"), businessKey("k", "b")))))
            .size(),
        1,
        "duplicate business key name");
    Assert.assertEquals(
        validate(
                new DeclaredKeys().setBusinessKeys(new DeclaredKeyArray(List.of(businessKey("k")))))
            .size(),
        1,
        "business key without fields");
  }
}
