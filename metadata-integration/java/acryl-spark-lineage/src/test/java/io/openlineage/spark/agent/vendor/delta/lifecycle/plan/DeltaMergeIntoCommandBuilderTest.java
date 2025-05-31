package io.openlineage.spark.agent.vendor.delta.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeltaMergeIntoCommandBuilderTest {

  private OpenLineageContext context;
  private DeltaMergeIntoCommandBuilder builder;
  private DatasetFactory<OpenLineage.OutputDataset> factory;

  @BeforeEach
  void setUp() {
    context = mock(OpenLineageContext.class);
    factory = mock(DatasetFactory.class);

    OpenLineage openLineage = mock(OpenLineage.class);
    when(context.getOpenLineage()).thenReturn(openLineage);

    // Setup the outputDataset factory mock
    when(context.getOutputDatasetBuilder()).thenReturn(factory);
    OpenLineage.OutputDataset mockDataset = mock(OpenLineage.OutputDataset.class);
    when(factory.getDataset("test_table", "delta")).thenReturn(mockDataset);

    builder = new DeltaMergeIntoCommandBuilder(context);
  }

  @Test
  void testApplyReturnsEmptyList() {
    LogicalPlan mockPlan = mock(LogicalPlan.class);
    List<OpenLineage.OutputDataset> result = builder.apply(mockPlan);
    assertEquals(Collections.emptyList(), result);
  }

  @Test
  void testIsDefinedAtReturnsFalseWhenNoDeltaClasses() {
    LogicalPlan mockPlan = mock(LogicalPlan.class);
    boolean result = builder.isDefinedAt(mockPlan);
    assertFalse(result);
  }
}
