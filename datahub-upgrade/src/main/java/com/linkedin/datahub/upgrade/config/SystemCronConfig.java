package com.linkedin.datahub.upgrade.config;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.cron.SystemUpdateCron;
import com.linkedin.datahub.upgrade.system.cron.steps.TweakReplicasStep;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
@Conditional(SystemUpdateCronCondition.class)
public class SystemCronConfig {

  @Autowired private ConditionChecker conditionChecker;

  @Bean(name = "TweakReplicasStep")
  @Conditional(SystemUpdateCronCondition.TweakReplicasStepCondition.class)
  public UpgradeStep tweakReplicasStep(
      List<ElasticSearchIndexed> services,
      Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    return new TweakReplicasStep(services, structuredProperties, conditionChecker.isDryRun());
  }

  //    //we would have other steps here...
  //    @Bean(name = "AnotherStep")
  //    public UpgradeStep anotherStep(List<ElasticSearchIndexed> services, Set<Pair<Urn,
  // StructuredPropertyDefinition>> structuredProperties) {
  //        //return new TweakReplicasStep(services, structuredProperties);
  //        throw new UnsupportedOperationException("not implemented");
  //    }
  //
  // create another such method, with a different conditional, to invoke a diff step
  @Bean(name = "systemUpdateCron")
  @Conditional(SystemUpdateCronCondition.TweakReplicasStepCondition.class)
  public SystemUpdateCron systemUpdateCron(
      @Qualifier("TweakReplicasStep") UpgradeStep tweakReplicasStep) {
    return new SystemUpdateCron(List.of(tweakReplicasStep));
  }
}
