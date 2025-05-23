package io.openlineage.spark.agent.vendor.hudi.lifecycle.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import scala.Option;

public class HudiMergeIntoCommandBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  HudiMergeIntoCommandBuilder builder;

  @Before
  public void setUp() {
    builder = new HudiMergeIntoCommandBuilder(context);
  }

  @Test
  public void testIsDefinedAt() {
    // Create a mock non-Hudi command
    LogicalPlan mockCommand = mock(LogicalPlan.class);
    when(mockCommand.getClass().getName())
        .thenReturn("org.apache.spark.sql.catalyst.plans.logical.LogicalPlan");

    // Test negative case
    assertFalse(builder.isDefinedAt(mockCommand));

    // Create a mock Hudi merge command
    LogicalPlan mockHudiMergeCommand = mock(LogicalPlan.class);
    when(mockHudiMergeCommand.getClass().getName())
        .thenReturn("org.apache.hudi.execution.MergeIntoHoodieTableCommand");
    when(mockHudiMergeCommand.getClass().getSimpleName()).thenReturn("MergeIntoHoodieTableCommand");

    // Test positive case
    assertTrue(builder.isDefinedAt(mockHudiMergeCommand));
  }

  @Test
  public void testApply() throws Exception {
    // Create a mock Hudi merge command
    LogicalPlan mockHudiMergeCommand = mock(LogicalPlan.class);
    when(mockHudiMergeCommand.getClass().getName())
        .thenReturn("org.apache.hudi.execution.MergeIntoHoodieTableCommand");
    when(mockHudiMergeCommand.getClass().getSimpleName()).thenReturn("MergeIntoHoodieTableCommand");

    // Create mock target table
    Object mockTargetTable = mock(Object.class);

    // Create mock TableIdentifier
    TableIdentifier mockTableId = mock(TableIdentifier.class);
    when(mockTableId.table()).thenReturn("testTable");
    when(mockTableId.database()).thenReturn(Option.apply("testDb"));

    // Create mock schema
    StructType mockSchema = mock(StructType.class);

    // Setup method reflection mocking
    Method targetTableMethod = mock(Method.class);
    Method findTableIdMethod = mock(Method.class);
    Method schemaMethod = mock(Method.class);

    // Override the findMethodContaining method
    builder =
        new HudiMergeIntoCommandBuilder(context) {
          protected Method findMethodContaining(Class<?> clazz, String nameContains) {
            if (nameContains.equals("targetTable") && clazz == mockHudiMergeCommand.getClass()) {
              return targetTableMethod;
            } else if (nameContains.equals("identifier") && clazz == mockTargetTable.getClass()) {
              return findTableIdMethod;
            } else if (nameContains.equals("schema") && clazz == mockTargetTable.getClass()) {
              return schemaMethod;
            }
            return null;
          }
        };

    // Setup invocation returns
    when(targetTableMethod.invoke(mockHudiMergeCommand)).thenReturn(mockTargetTable);
    when(findTableIdMethod.invoke(mockTargetTable)).thenReturn(mockTableId);
    when(schemaMethod.invoke(mockTargetTable)).thenReturn(mockSchema);

    // Mock dataset factory for output
    OpenLineage.OutputDataset mockDataset = mock(OpenLineage.OutputDataset.class);
    OpenLineage.OutputDatasetBuilder mockBuilder = mock(OpenLineage.OutputDatasetBuilder.class);
    when(mockBuilder.name("testDb.testTable")).thenReturn(mockBuilder);
    when(mockBuilder.namespace("hudi")).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockDataset);

    // Setup the builder with the correct return type
    builder =
        new HudiMergeIntoCommandBuilder(context) {
          protected OpenLineage.OutputDatasetBuilder outputDataset() {
            return mockBuilder;
          }

          protected Method findMethodContaining(Class<?> clazz, String nameContains) {
            if (nameContains.equals("targetTable") && clazz == mockHudiMergeCommand.getClass()) {
              return targetTableMethod;
            } else if (nameContains.equals("identifier") && clazz == mockTargetTable.getClass()) {
              return findTableIdMethod;
            } else if (nameContains.equals("schema") && clazz == mockTargetTable.getClass()) {
              return schemaMethod;
            }
            return null;
          }
        };

    // Test the apply method
    List<OpenLineage.OutputDataset> result = builder.apply(mockHudiMergeCommand);

    // Verify results
    assertEquals(1, result.size());
    assertEquals(mockDataset, result.get(0));
  }
}
