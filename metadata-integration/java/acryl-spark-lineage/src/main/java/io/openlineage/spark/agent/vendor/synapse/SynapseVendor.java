package io.openlineage.spark.agent.vendor.synapse;

import static io.openlineage.spark.agent.vendor.synapse.Constants.*;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import io.openlineage.spark.agent.vendor.synapse.lifecycle.SynapseVisitorFactory;
import io.openlineage.spark.agent.vendor.synapse.lifecycle.plan.SynapseEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SynapseVendor implements Vendor {

  public static boolean hasSynapseClasses() {
    try {
      Thread.currentThread().getContextClassLoader().loadClass(SYNAPSE_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - Synapse classes not available
    }
    return false;
  }

  @Override
  public boolean isVendorAvailable() {
    log.info("Checking if Azure Synapse classes are available");
    return hasSynapseClasses();
  }

  @Override
  public Optional<VisitorFactory> getVisitorFactory() {
    return Optional.of(new SynapseVisitorFactory());
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new SynapseEventHandlerFactory());
  }
}
