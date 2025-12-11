/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.upgrade.conditions;

import java.util.Objects;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class ReindexDebugCondition implements Condition {
  public static final String DEBUG_REINDEX = "ReindexDebug";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    ApplicationArguments bean = context.getBeanFactory().getBean(ApplicationArguments.class);
    return bean.getNonOptionArgs().stream()
        .filter(Objects::nonNull)
        .anyMatch(DEBUG_REINDEX::equalsIgnoreCase);
  }
}
