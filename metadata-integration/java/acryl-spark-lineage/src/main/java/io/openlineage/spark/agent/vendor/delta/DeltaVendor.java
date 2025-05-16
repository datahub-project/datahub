package io.openlineage.spark.agent.vendor.delta;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import io.openlineage.spark.agent.vendor.delta.lifecycle.DeltaVisitorFactory;
import io.openlineage.spark.agent.vendor.delta.lifecycle.plan.DeltaEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeltaVendor implements Vendor {

  public static final String DELTA_MERGE_INTO_COMMAND =
      "io.delta.tables.execution.DeltaMergeIntoCommand";

  public static boolean hasDeltaClasses() {
    try {
      Thread.currentThread().getContextClassLoader().loadClass(DELTA_MERGE_INTO_COMMAND);
      return true;
    } catch (Exception e) {
      // swallow - Delta classes not available
    }
    return false;
  }

  @Override
  public boolean isVendorAvailable() {
    log.info("Checking if Delta Lake classes are available");
    return hasDeltaClasses();
  }

  @Override
  public Optional<VisitorFactory> getVisitorFactory() {
    return Optional.of(new DeltaVisitorFactory());
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new DeltaEventHandlerFactory());
  }
}
