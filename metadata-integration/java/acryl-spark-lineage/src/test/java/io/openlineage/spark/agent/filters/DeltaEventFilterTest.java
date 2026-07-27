package io.openlineage.spark.agent.filters;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.commands.MergeIntoCommand;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DeltaEventFilterTest {

  @Test
  void allowsMergeJobEvents() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    LogicalPlan mergePlan = mock(MergeIntoCommand.class);

    try (MockedStatic<EventFilterUtils> filterUtils = mockStatic(EventFilterUtils.class)) {
      filterUtils.when(EventFilterUtils::isDeltaPlan).thenReturn(true);
      filterUtils
          .when(() -> EventFilterUtils.getLogicalPlan(context))
          .thenReturn(Optional.of(mergePlan));

      assertFalse(new DeltaEventFilter(context).isDisabled(mock(SparkListenerJobStart.class)));
    }
  }

  @Test
  void filtersNonMergeDeltaJobEvents() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    LogicalPlan plan = mock(Filter.class);

    try (MockedStatic<EventFilterUtils> filterUtils = mockStatic(EventFilterUtils.class)) {
      filterUtils.when(EventFilterUtils::isDeltaPlan).thenReturn(true);
      filterUtils
          .when(() -> EventFilterUtils.getLogicalPlan(context))
          .thenReturn(Optional.of(plan));

      assertTrue(new DeltaEventFilter(context).isDisabled(mock(SparkListenerJobStart.class)));
    }
  }

  @Test
  void recognizesOpenSourceAndDatabricksMergeCommands() {
    assertTrue(
        DeltaEventFilter.isMergeIntoCommandClass(
            "org.apache.spark.sql.delta.commands.MergeIntoCommand"));
    assertTrue(
        DeltaEventFilter.isMergeIntoCommandClass(
            "com.databricks.sql.transaction.tahoe.commands.MergeIntoCommandEdge"));
    assertFalse(
        DeltaEventFilter.isMergeIntoCommandClass(
            "org.apache.spark.sql.delta.commands.MergeIntoCommandHelper"));
  }
}
