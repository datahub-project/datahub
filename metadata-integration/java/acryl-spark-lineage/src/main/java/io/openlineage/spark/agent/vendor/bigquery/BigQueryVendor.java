package io.openlineage.spark.agent.vendor.bigquery;

import static io.openlineage.spark.agent.vendor.bigquery.Constants.*;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import io.openlineage.spark.agent.vendor.bigquery.lifecycle.BigQueryVisitorFactory;
import io.openlineage.spark.agent.vendor.bigquery.lifecycle.plan.BigQueryEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BigQueryVendor implements Vendor {

  public static boolean hasBigQueryClasses() {
    try {
      Thread.currentThread().getContextClassLoader().loadClass(BIGQUERY_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - BigQuery classes not available
    }
    return false;
  }

  @Override
  public boolean isVendorAvailable() {
    log.info("Checking if BigQuery classes are available");
    return hasBigQueryClasses();
  }

  @Override
  public Optional<VisitorFactory> getVisitorFactory() {
    return Optional.of(new BigQueryVisitorFactory());
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new BigQueryEventHandlerFactory());
  }
}
