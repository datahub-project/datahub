package io.openlineage.spark.agent.vendor.redshift;

import static io.openlineage.spark.agent.vendor.redshift.Constants.*;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import io.openlineage.spark.agent.vendor.redshift.lifecycle.RedshiftRelationVisitor;
import io.openlineage.spark.agent.vendor.redshift.lifecycle.plan.RedshiftEventHandlerFactory;
import io.openlineage.spark.agent.vendor.snowflake.lifecycle.SnowflakeVisitorFactory;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedshiftVendor implements Vendor {

  public static boolean hasRedshiftClasses() {
    /*
     Checking the Redshift class with both
     SnowflakeRelationVisitor.class.getClassLoader.loadClass and
     Thread.currentThread().getContextClassLoader().loadClass. The first checks if the class is
     present on the classpath, and the second one is a catchall which captures if the class has
     been installed. This is relevant for Azure Databricks where jars can be installed and
     accessible to the user, even if they are not present on the classpath.
    */
    try {
      RedshiftRelationVisitor.class.getClassLoader().loadClass(REDSHIFT_PROVIDER_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - we don't care
    }
    try {
      Thread.currentThread().getContextClassLoader().loadClass(REDSHIFT_PROVIDER_CLASS_NAME);
      return true;
    } catch (Exception e) {
      // swallow - we don't care
    }
    return false;
  }

  @Override
  public boolean isVendorAvailable() {
    log.info("Checking if Redshift classes are available");
    return hasRedshiftClasses();
  }

  @Override
  public Optional<VisitorFactory> getVisitorFactory() {
    return Optional.of(new SnowflakeVisitorFactory());
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new RedshiftEventHandlerFactory());
  }
}
