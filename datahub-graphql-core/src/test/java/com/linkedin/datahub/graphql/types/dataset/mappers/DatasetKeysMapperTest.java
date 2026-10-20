package com.linkedin.datahub.graphql.types.dataset.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.DatasetKeys;
import com.linkedin.datahub.graphql.generated.PrimaryKeySource;
import com.linkedin.dataset.DeclaredKey;
import com.linkedin.dataset.DeclaredKeyArray;
import com.linkedin.dataset.DeclaredKeys;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class DatasetKeysMapperTest {

  private static SchemaMetadata schema(List<String> primaryKeys, String... partOfKeyFields) {
    SchemaFieldArray fields = new SchemaFieldArray();
    fields.add(new SchemaField().setFieldPath("col_a").setIsPartOfKey(false));
    for (String path : partOfKeyFields) {
      fields.add(new SchemaField().setFieldPath(path).setIsPartOfKey(true));
    }
    SchemaMetadata schema = new SchemaMetadata().setFields(fields);
    if (primaryKeys != null) {
      schema.setPrimaryKeys(new StringArray(primaryKeys));
    }
    return schema;
  }

  private static DeclaredKeys declared(String... primaryKey) {
    return new DeclaredKeys().setPrimaryKey(new StringArray(List.of(primaryKey)));
  }

  @DataProvider
  public Object[][] precedence() {
    return new Object[][] {
      // source PK, declared PK, effective PK, source of effective PK, conflict
      {schema(List.of("id")), null, List.of("id"), PrimaryKeySource.SOURCE, false},
      {schema(null), declared("id"), List.of("id"), PrimaryKeySource.DECLARED, false},
      {null, declared("id"), List.of("id"), PrimaryKeySource.DECLARED, false},
      {
        schema(List.of("id", "day")),
        declared("day", "id"),
        List.of("id", "day"),
        PrimaryKeySource.SOURCE,
        false
      },
      {schema(List.of("id")), declared("col_a"), List.of("id"), PrimaryKeySource.SOURCE, true},
      {schema(null), null, List.of(), PrimaryKeySource.NONE, false},
      // isPartOfKey fields count as the source key too
      {schema(null, "id"), declared("col_a"), List.of("id"), PrimaryKeySource.SOURCE, true},
    };
  }

  @Test(dataProvider = "precedence")
  public void testPrecedence(
      SchemaMetadata schemaMetadata,
      DeclaredKeys declaredKeys,
      List<String> effective,
      PrimaryKeySource source,
      boolean conflict) {
    DatasetKeys keys = DatasetKeysMapper.map(schemaMetadata, declaredKeys);

    assertEquals(keys.getEffectivePrimaryKey(), effective);
    assertEquals(keys.getPrimaryKeySource(), source);
    assertEquals(keys.getConflict(), conflict);
  }

  @Test
  public void testSourceKeyCombinesPrimaryKeysAndPartOfKeyFields() {
    DatasetKeys keys = DatasetKeysMapper.map(schema(List.of("id"), "id", "day"), null);

    assertEquals(keys.getEffectivePrimaryKey(), List.of("id", "day"));
    assertNull(keys.getDeclaredPrimaryKey());
  }

  @Test
  public void testBusinessKeysAreReturnedIndependentlyOfPrimaryKey() {
    DeclaredKeys declaredKeys =
        new DeclaredKeys()
            .setBusinessKeys(
                new DeclaredKeyArray(
                    List.of(
                        new DeclaredKey()
                            .setName("natural")
                            .setFields(new StringArray(List.of("col_a", "day")))
                            .setDescription("One row per account per day"))));

    DatasetKeys keys = DatasetKeysMapper.map(schema(List.of("id")), declaredKeys);

    assertEquals(keys.getPrimaryKeySource(), PrimaryKeySource.SOURCE);
    assertFalse(keys.getConflict());
    assertEquals(keys.getBusinessKeys().size(), 1);
    assertEquals(keys.getBusinessKeys().get(0).getFields(), List.of("col_a", "day"));
    assertTrue(keys.getBusinessKeys().get(0).getDescription().startsWith("One row"));
  }
}
