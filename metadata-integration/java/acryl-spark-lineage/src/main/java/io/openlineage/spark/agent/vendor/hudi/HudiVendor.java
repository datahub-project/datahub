package io.openlineage.spark.agent.vendor.hudi;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import io.openlineage.spark.agent.vendor.hudi.lifecycle.HudiVisitorFactory;
import io.openlineage.spark.agent.vendor.hudi.lifecycle.plan.HudiEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HudiVendor implements Vendor {

  public static final String HUDI_MERGE_INTO_COMMAND =
      "org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitioner";

  public static boolean hasHudiClasses() {
    try {
      Thread.currentThread().getContextClassLoader().loadClass(HUDI_MERGE_INTO_COMMAND);
      return true;
    } catch (Exception e) {
      // swallow - Hudi classes not available
    }
    return false;
  }

  @Override
  public boolean isVendorAvailable() {
    log.info("Checking if Hudi classes are available");
    return hasHudiClasses();
  }

  @Override
  public Optional<VisitorFactory> getVisitorFactory() {
    return Optional.of(new HudiVisitorFactory());
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new HudiEventHandlerFactory());
  }
}
