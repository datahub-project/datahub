package io.openlineage.spark.agent.vendor.hudi.lifecycle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import scala.Option;

public class HudiCommandVisitorTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  DatasetFactory<OpenLineage.OutputDataset> factory = mock(DatasetFactory.class);
  HudiCommandVisitor<OpenLineage.OutputDataset> visitor;

  @Before
  public void setUp() {
    visitor = new HudiCommandVisitor<>(context, factory);
  }

  @Test
  public void testIsDefinedAt() throws Exception {
    // Mock a normal LogicalPlan (not Hudi)
    LogicalPlan mockPlan = mock(LogicalPlan.class);
    when(mockPlan.getClass().getName())
        .thenReturn("org.apache.spark.sql.catalyst.plans.logical.LogicalPlan");

    // Test negative case
    assertFalse(visitor.isDefinedAt(mockPlan));
  }

  @Test
  public void testApply() throws Exception {
    // Create a mock Hudi command
    LogicalPlan mockHudiCommand = mock(LogicalPlan.class);
    when(mockHudiCommand.getClass().getName())
        .thenReturn("org.apache.hudi.execution.bulkinsert.BulkInsertCommand");

    // Create mock table object
    Object mockTable = mock(Object.class);

    // Create mock TableIdentifier
    TableIdentifier mockTableId = mock(TableIdentifier.class);
    when(mockTableId.table()).thenReturn("testTable");
    when(mockTableId.database()).thenReturn(Option.apply("testDb"));

    // Create mock schema
    StructType mockSchema = mock(StructType.class);

    // Create a mock method for getting the table
    Method tableMethod = mock(Method.class);
    // Mock the findMethodContaining to return our mocked method
    Method findMethod =
        HudiCommandVisitor.class.getDeclaredMethod(
            "findMethodContaining", Class.class, String.class);
    findMethod.setAccessible(true);

    // Define the methods we need
    final Method findTableIdMethod = mock(Method.class);
    final Method schemaMethod = mock(Method.class);

    // Setup reflection mocking
    visitor =
        new HudiCommandVisitor<OpenLineage.OutputDataset>(context, factory) {
          protected Method findMethodContaining(Class<?> clazz, String nameContains) {
            if (nameContains.equals("table") && clazz == mockHudiCommand.getClass()) {
              return tableMethod;
            } else if (nameContains.equals("identifier") && clazz == mockTable.getClass()) {
              return findTableIdMethod;
            } else if (nameContains.equals("schema") && clazz == mockTable.getClass()) {
              return schemaMethod;
            }
            return null;
          }
        };

    // Setup invocation returns
    when(tableMethod.invoke(mockHudiCommand)).thenReturn(mockTable);
    when(findTableIdMethod.invoke(mockTable)).thenReturn(mockTableId);
    when(schemaMethod.invoke(mockTable)).thenReturn(mockSchema);

    // Mock the dataset factory
    OpenLineage.OutputDataset mockDataset = mock(OpenLineage.OutputDataset.class);
    when(factory.getDataset("testDb.testTable", "hudi", mockSchema)).thenReturn(mockDataset);

    // Test the apply method
    List<OpenLineage.OutputDataset> result = visitor.apply(mockHudiCommand);

    // Verify results
    assertEquals(1, result.size());
    assertEquals(mockDataset, result.get(0));
  }
}
