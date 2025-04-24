package com.linkedin.datahub.upgrade.config;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.cron.SystemUpdateCron;
import com.linkedin.datahub.upgrade.system.cron.steps.TweakReplicasStep;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Set;

@Configuration
@Conditional(SystemUpdateCronCondition.class)
public class SystemCronConfig {

    @Bean(name = "TweakReplicasStep")
    public UpgradeStep tweakReplicasStep(List<ElasticSearchIndexed> services, Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
        return new TweakReplicasStep(services, structuredProperties);
    }

    @Bean(name = "AnotherStep")
    public UpgradeStep anotherStep(List<ElasticSearchIndexed> services, Set<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
        return new TweakReplicasStep(services, structuredProperties);
    }

    @Bean(name = "systemUpdateCron")
    public SystemUpdateCron systemUpdateCron(@Qualifier("TweakReplicasStep") UpgradeStep tweakReplicasStep) {
        // TODO  consider which step(s) to run via additional arg
        return new SystemUpdateCron(List.of(tweakReplicasStep));
    }
}
