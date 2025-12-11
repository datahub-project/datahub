/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.conditions;

import static com.linkedin.datahub.upgrade.conditions.LoadIndicesCondition.LOAD_INDICES_ARG;
import static com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.SQL_SETUP_ARG;

import java.util.Objects;
import java.util.Set;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class GeneralUpgradeCondition implements Condition {
  public static final Set<String> EXCLUDED_ARGS = Set.of(LOAD_INDICES_ARG, SQL_SETUP_ARG);

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    // This condition matches when LoadIndices is NOT in the arguments
    return !context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs().stream()
        .filter(Objects::nonNull)
        .anyMatch(EXCLUDED_ARGS::contains);
  }
}
