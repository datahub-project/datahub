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
import java.util.Set;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class SystemUpdateCondition implements Condition {
  public static final String SYSTEM_UPDATE_ARG = "SystemUpdate";
  public static final String BLOCKING_SYSTEM_UPDATE_ARG = SYSTEM_UPDATE_ARG + "Blocking";
  public static final String NONBLOCKING_SYSTEM_UPDATE_ARG = SYSTEM_UPDATE_ARG + "NonBlocking";
  public static final Set<String> SYSTEM_UPDATE_ARGS =
      Set.of(SYSTEM_UPDATE_ARG, BLOCKING_SYSTEM_UPDATE_ARG, NONBLOCKING_SYSTEM_UPDATE_ARG);

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs().stream()
        .filter(Objects::nonNull)
        .anyMatch(SYSTEM_UPDATE_ARGS::contains);
  }

  public static class BlockingSystemUpdateCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
      return context
          .getBeanFactory()
          .getBean(ApplicationArguments.class)
          .getNonOptionArgs()
          .stream()
          .anyMatch(arg -> SYSTEM_UPDATE_ARG.equals(arg) || BLOCKING_SYSTEM_UPDATE_ARG.equals(arg));
    }
  }

  public static class NonBlockingSystemUpdateCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
      return context
          .getBeanFactory()
          .getBean(ApplicationArguments.class)
          .getNonOptionArgs()
          .stream()
          .anyMatch(
              arg -> SYSTEM_UPDATE_ARG.equals(arg) || NONBLOCKING_SYSTEM_UPDATE_ARG.equals(arg));
    }
  }
}
