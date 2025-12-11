/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.config;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.conditions.SystemUpdateCronCondition;
import com.linkedin.datahub.upgrade.system.cron.SystemUpdateCron;
import com.linkedin.datahub.upgrade.system.cron.steps.TweakReplicasStep;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Set;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCronCondition.class)
public class SystemCronConfig {

  @Bean(name = "TweakReplicasStep")
  public UpgradeStep tweakReplicasStep(
      List<ElasticSearchIndexed> services,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    return new TweakReplicasStep(services, structuredProperties);
  }

  // create another such method, with a different conditional, to invoke a diff step
  @Bean(name = "systemUpdateCron")
  public SystemUpdateCron systemUpdateCron(
      @Qualifier("TweakReplicasStep") UpgradeStep tweakReplicasStep) {
    return new SystemUpdateCron(List.of(tweakReplicasStep));
  }
}
