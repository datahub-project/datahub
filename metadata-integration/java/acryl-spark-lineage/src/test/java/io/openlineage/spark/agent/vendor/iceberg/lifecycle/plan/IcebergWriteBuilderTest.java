package io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

public class IcebergWriteBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  IcebergWriteBuilder builder;

  @Before
  public void setUp() {
    builder = new IcebergWriteBuilder(context);
  }

  @Test
  public void testIsDefinedAt() {
    // Create a mock non-Iceberg command
    LogicalPlan mockCommand = mock(LogicalPlan.class);
    when(mockCommand.getClass().getName())
        .thenReturn("org.apache.spark.sql.catalyst.plans.logical.LogicalPlan");
    when(mockCommand.getClass().getSimpleName()).thenReturn("LogicalPlan");

    // Test negative case
    assertFalse(builder.isDefinedAt(mockCommand));

    // Create a mock Iceberg write command
    LogicalPlan mockIcebergCommand = mock(LogicalPlan.class);
    when(mockIcebergCommand.getClass().getName())
        .thenReturn("org.apache.iceberg.spark.writes.WriteFiles");
    when(mockIcebergCommand.getClass().getSimpleName()).thenReturn("WriteFiles");

    // Test positive case
    assertTrue(builder.isDefinedAt(mockIcebergCommand));
  }

  @Test
  public void testApply() throws Exception {
    // Create a mock Iceberg write command
    LogicalPlan mockIcebergCommand = mock(LogicalPlan.class);
    when(mockIcebergCommand.getClass().getName())
        .thenReturn("org.apache.iceberg.spark.writes.WriteFiles");
    when(mockIcebergCommand.getClass().getSimpleName()).thenReturn("WriteFiles");

    // Create mock table object
    Object mockTable = mock(Object.class);

    // Create mock schema
    StructType mockSchema = mock(StructType.class);

    // Create mocked methods for reflection
    Method getTableMethod = mock(Method.class);

    // Create a builder with overridden helper methods for testing
    builder =
        new IcebergWriteBuilder(context) {
          protected Method findMethodByNamePattern(Class<?> clazz, String pattern) {
            if (pattern.equals("table|destination") && clazz == mockIcebergCommand.getClass()) {
              return getTableMethod;
            }
            return null;
          }

          protected String extractTableName(Object tableObj) {
            return "testTable";
          }

          protected String extractNamespace(Object tableObj) {
            return "testDb";
          }

          protected StructType extractSchema(Object tableObj) {
            return mockSchema;
          }
        };

    // Mock invocation returns
    when(getTableMethod.invoke(mockIcebergCommand)).thenReturn(mockTable);

    // Mock output dataset builder and dataset
    OpenLineage.OutputDataset mockDataset = mock(OpenLineage.OutputDataset.class);
    OpenLineage.OutputDatasetBuilder mockBuilder = mock(OpenLineage.OutputDatasetBuilder.class);
    when(mockBuilder.name("testDb.testTable")).thenReturn(mockBuilder);
    when(mockBuilder.namespace("iceberg")).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockDataset);

    // Setup the builder's outputDataset method with correct return type
    builder =
        new IcebergWriteBuilder(context) {
          protected OpenLineage.OutputDatasetBuilder outputDataset() {
            return mockBuilder;
          }

          protected Method findMethodByNamePattern(Class<?> clazz, String pattern) {
            if (pattern.equals("table|destination") && clazz == mockIcebergCommand.getClass()) {
              return getTableMethod;
            }
            return null;
          }

          protected String extractTableName(Object tableObj) {
            return "testTable";
          }

          protected String extractNamespace(Object tableObj) {
            return "testDb";
          }

          protected StructType extractSchema(Object tableObj) {
            return mockSchema;
          }
        };

    // Setup invocation returns
    when(getTableMethod.invoke(mockIcebergCommand)).thenReturn(mockTable);

    // Test the apply method
    List<OpenLineage.OutputDataset> result = builder.apply(mockIcebergCommand);

    // Verify results
    assertEquals(1, result.size());
    assertEquals(mockDataset, result.get(0));
  }
}
