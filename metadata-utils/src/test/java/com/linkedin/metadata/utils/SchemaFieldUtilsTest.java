package com.linkedin.metadata.utils;

import static org.testng.Assert.assertEquals;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

/** Tests the capabilities of {@link EntityKeyUtils} */
public class SchemaFieldUtilsTest {
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD)");

  @Test
  public void testGenerateSchemaFieldURN() {
    assertEquals(
        SchemaFieldUtils.generateSchemaFieldUrn(TEST_DATASET_URN, "customer_id"),
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),customer_id)"));

    assertEquals(
        SchemaFieldUtils.generateSchemaFieldUrn(
            TEST_DATASET_URN,
            "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f"),
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f)"));
  }

  @Test
  public void testDowngradeSchemaFieldUrn() {
    assertEquals(
        SchemaFieldUtils.downgradeSchemaFieldUrn(
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),customer_id)")),
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),customer_id)"),
        "Expected no change to v1 field path schemaField URN");

    assertEquals(
        SchemaFieldUtils.downgradeSchemaFieldUrn(
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f)")),
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),a.f)"),
        "Expected downgrade to v1 field path schemaField URN");
  }

  @Test
  public void testSchemaFieldAliases() {
    SchemaMetadata testSchema = new SchemaMetadata();
    testSchema.setFields(
        new SchemaFieldArray(
            List.of(
                new SchemaField().setFieldPath("customer_id"),
                new SchemaField().setFieldPath("[version=2.0].[type=ABFooUnion].[type=union].a"),
                new SchemaField()
                    .setFieldPath("[version=2.0].[type=ABFooUnion].[type=union].[type=A].a"),
                new SchemaField()
                    .setFieldPath(
                        "[version=2.0].[type=ABFooUnion].[type=union].[type=A].a.[type=string].f"),
                new SchemaField()
                    .setFieldPath("[version=2.0].[type=ABFooUnion].[type=union].[type=B].a"),
                new SchemaField()
                    .setFieldPath(
                        "[version=2.0].[type=ABFooUnion].[type=union].[type=B].a.[type=string].f"),
                new SchemaField()
                    .setFieldPath(
                        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a"),
                new SchemaField()
                    .setFieldPath(
                        "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f"))));

    assertEquals(
        SchemaFieldUtils.getSchemaFieldAliases(
            TEST_DATASET_URN, testSchema, new SchemaField().setFieldPath("customer_id")),
        Set.of(
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),customer_id)")),
        "Expected only the target v1 schemaField and no aliases");

    assertEquals(
        SchemaFieldUtils.getSchemaFieldAliases(
            TEST_DATASET_URN,
            testSchema,
            new SchemaField().setFieldPath("[version=2.0].[type=ABFooUnion].[type=union].a")),
        Set.of(
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),[version=2.0].[type=ABFooUnion].[type=union].a)"),
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),a)"),
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),[version=2.0].[type=ABFooUnion].[type=union].[type=A].a)"),
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),[version=2.0].[type=ABFooUnion].[type=union].[type=B].a)"),
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a)")),
        "Expected one v1 alias and 3 collisions. Excluding target");

    assertEquals(
        SchemaFieldUtils.getSchemaFieldAliases(
            TEST_DATASET_URN,
            testSchema,
            new SchemaField()
                .setFieldPath(
                    "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f")),
        Set.of(
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].f)"),
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),a.f)"),
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),[version=2.0].[type=ABFooUnion].[type=union].[type=A].a.[type=string].f)"),
            UrnUtils.getUrn(
                "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.customers,PROD),[version=2.0].[type=ABFooUnion].[type=union].[type=B].a.[type=string].f)")),
        "Expected one v1 alias and 2 collisions. Excluding target");

    assertEquals(
        SchemaFieldUtils.getSchemaFieldAliases(
            TEST_DATASET_URN,
            testSchema,
            new SchemaField()
                .setFieldPath(
                    "[version=2.0].[type=ABFooUnion].[type=union].[type=array].[type=array].[type=Foo].a.[type=long].z")),
        Set.of(),
        "Expected no aliases since target field is not present.");
  }
}
