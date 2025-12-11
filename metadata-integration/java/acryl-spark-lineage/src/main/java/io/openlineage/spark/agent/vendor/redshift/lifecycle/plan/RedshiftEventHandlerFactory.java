/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.openlineage.spark.agent.vendor.redshift.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import scala.PartialFunction;

public class RedshiftEventHandlerFactory implements OpenLineageEventHandlerFactory {
  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      createOutputDatasetBuilder(OpenLineageContext context) {
    // The right function will be determined at runtime by using type checking based on the correct
    // Spark LogicalPlan
    return Collections.singleton(
        (PartialFunction) new RedshiftSaveIntoDataSourceCommandBuilder(context));
  }
}
