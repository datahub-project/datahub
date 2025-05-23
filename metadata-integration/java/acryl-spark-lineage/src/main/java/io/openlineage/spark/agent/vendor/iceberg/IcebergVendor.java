package io.openlineage.spark.agent.vendor.iceberg;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import io.openlineage.spark.agent.vendor.iceberg.lifecycle.IcebergVisitorFactory;
import io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan.IcebergEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IcebergVendor implements Vendor {

  public static final String ICEBERG_TABLE_CLASS = "org.apache.iceberg.spark.SparkTable";

  public static boolean hasIcebergClasses() {
    try {
      Thread.currentThread().getContextClassLoader().loadClass(ICEBERG_TABLE_CLASS);
      return true;
    } catch (Exception e) {
      // swallow - Iceberg classes not available
    }
    return false;
  }

  @Override
  public boolean isVendorAvailable() {
    log.info("Checking if Iceberg classes are available");
    return hasIcebergClasses();
  }

  @Override
  public Optional<VisitorFactory> getVisitorFactory() {
    return Optional.of(new IcebergVisitorFactory());
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new IcebergEventHandlerFactory());
  }
}
