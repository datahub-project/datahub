package io.openlineage.spark.agent.vendor.delta.lifecycle.plan;

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

public class DeltaMergeIntoCommandBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class);
  DeltaMergeIntoCommandBuilder builder;

  @Before
  public void setUp() {
    builder = new DeltaMergeIntoCommandBuilder(context);
  }

  @Test
  public void testIsDefinedAt() {
    // Create a mock non-Delta command
    LogicalPlan mockCommand = mock(LogicalPlan.class);

    // Test negative case
    assertFalse(builder.isDefinedAt(mockCommand));
  }

  @Test
  public void testApply() throws Exception {
    // Create a mock DeltaMergeIntoCommand
    LogicalPlan mockDeltaMergeCommand = mock(LogicalPlan.class);

    // Create mock Target table
    Object mockTarget = mock(Object.class);

    // Create mock TableIdentifier
    TableIdentifier mockTableId = mock(TableIdentifier.class);
    when(mockTableId.table()).thenReturn("testTable");
    when(mockTableId.database()).thenReturn(Option.apply("testDb"));

    // Create mock schema
    StructType mockSchema = mock(StructType.class);

    // Setup method reflection mocking
    Method targetMethod = mock(Method.class);
    when(mockDeltaMergeCommand.getClass().getMethod("target")).thenReturn(targetMethod);
    when(targetMethod.invoke(mockDeltaMergeCommand)).thenReturn(mockTarget);

    Method tableIdMethod = mock(Method.class);
    when(mockTarget.getClass().getMethod("tableIdentifier")).thenReturn(tableIdMethod);
    when(tableIdMethod.invoke(mockTarget)).thenReturn(mockTableId);

    Method schemaMethod = mock(Method.class);
    when(mockTarget.getClass().getMethod("schema")).thenReturn(schemaMethod);
    when(schemaMethod.invoke(mockTarget)).thenReturn(mockSchema);

    // Mock dataset factory
    DatasetFactory<OpenLineage.OutputDataset> factory = mock(DatasetFactory.class);
    OpenLineage.OutputDataset mockDataset = mock(OpenLineage.OutputDataset.class);

    // Setup the builder's outputDataset method
    builder =
        new DeltaMergeIntoCommandBuilder(context) {
          @Override
          protected DatasetFactory<OpenLineage.OutputDataset> outputDataset() {
            return factory;
          }
        };

    when(factory.getDataset("testDb.testTable", "delta", mockSchema)).thenReturn(mockDataset);

    // Test the apply method
    List<OpenLineage.OutputDataset> result = builder.apply(mockDeltaMergeCommand);

    // Verify results
    assertEquals(1, result.size());
    assertEquals(mockDataset, result.get(0));
  }
}
