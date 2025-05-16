package io.openlineage.spark.agent.vendor.iceberg.lifecycle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;

public class IcebergCommandVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  DatasetFactory<OpenLineage.OutputDataset> factory = mock(DatasetFactory.class);
  IcebergCommandVisitor<OpenLineage.OutputDataset> visitor;

  @Before
  public void setUp() {
    visitor = new IcebergCommandVisitor<>(context, factory);
  }

  @Test
  public void testIsDefinedAt() throws Exception {
    // Mock a normal LogicalPlan (not Iceberg)
    LogicalPlan mockPlan = mock(LogicalPlan.class);
    when(mockPlan.getClass().getName())
        .thenReturn("org.apache.spark.sql.catalyst.plans.logical.LogicalPlan");

    // Test negative case
    assertFalse(visitor.isDefinedAt(mockPlan));

    // Mock an Iceberg LogicalPlan
    LogicalPlan mockIcebergPlan = mock(LogicalPlan.class);
    when(mockIcebergPlan.getClass().getName())
        .thenReturn("org.apache.iceberg.spark.procedures.IcebergProcedure");

    // Test positive case
    assertTrue(visitor.isDefinedAt(mockIcebergPlan));
  }

  @Test
  public void testApply() throws Exception {
    // Create a mock Iceberg command
    LogicalPlan mockIcebergCommand = mock(LogicalPlan.class);
    when(mockIcebergCommand.getClass().getName())
        .thenReturn("org.apache.iceberg.spark.actions.RewriteManifestsAction");

    // Create mock table object
    Object mockTable = mock(Object.class);

    // Create mock schema
    StructType mockSchema = mock(StructType.class);

    // Create mock method for getting table name and namespace
    Method getTableMethod = mock(Method.class);
    Method getNameMethod = mock(Method.class);
    Method getSchemaMethod = mock(Method.class);
    Method getNamespaceMethod = mock(Method.class);

    // Mock the findMethodByNamePart to return our mocked methods
    visitor =
        new IcebergCommandVisitor<OpenLineage.OutputDataset>(context, factory) {
          protected Method findMethodByNamePart(Class<?> clazz, String namePart) {
            if (namePart.equals("table") && clazz == mockIcebergCommand.getClass()) {
              return getTableMethod;
            } else if (namePart.equals("name") && clazz == mockTable.getClass()) {
              return getNameMethod;
            } else if (namePart.equals("schema") && clazz == mockTable.getClass()) {
              return getSchemaMethod;
            } else if (namePart.equals("namespace") && clazz == mockTable.getClass()) {
              return getNamespaceMethod;
            }
            return null;
          }
        };

    // Mock method invocation returns
    when(getTableMethod.invoke(mockIcebergCommand)).thenReturn(mockTable);
    when(getNameMethod.invoke(mockTable)).thenReturn("testTable");
    when(getSchemaMethod.invoke(mockTable)).thenReturn(mockSchema);
    when(getNamespaceMethod.invoke(mockTable)).thenReturn("testDb");

    // Mock the dataset factory
    OpenLineage.OutputDataset mockDataset = mock(OpenLineage.OutputDataset.class);
    when(factory.getDataset("testDb.testTable", "iceberg", mockSchema)).thenReturn(mockDataset);

    // Test the apply method
    List<OpenLineage.OutputDataset> result = visitor.apply(mockIcebergCommand);

    // Verify results
    assertEquals(1, result.size());
    assertEquals(mockDataset, result.get(0));
  }
}
