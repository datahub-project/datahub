package io.openlineage.spark.api;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.spark.agent.vendor.redshift.lifecycle.plan.RedshiftEventHandlerFactory;
import org.junit.jupiter.api.Test;

/**
 * DataHub's {@code Vendors} copy is the only place the Redshift vendor is registered: upstream's
 * vendor list has no Redshift entry, and vendors are loaded by class name from that list, not
 * through a service loader. If an OpenLineage upgrade replaced the copy with upstream's, Redshift
 * writes would silently stop producing outputs.
 */
class VendorsTest {

  @Test
  void redshiftVendorIsLoadedWhenSparkRedshiftIsOnTheClasspath() {
    assertTrue(
        Vendors.getVendors().getEventHandlerFactories().stream()
            .anyMatch(RedshiftEventHandlerFactory.class::isInstance),
        "the Redshift vendor's event handler factory should be registered");
  }
}
