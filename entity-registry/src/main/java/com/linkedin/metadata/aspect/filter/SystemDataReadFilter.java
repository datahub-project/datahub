package com.linkedin.metadata.aspect.filter;

import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.filter.AspectReadContext;
import com.linkedin.metadata.aspect.plugins.filter.AspectReadFilter;
import com.linkedin.metadata.aspect.plugins.filter.ReadIntent;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.policy.SystemDataPolicy;
import com.linkedin.metadata.policy.SystemDataPolicyIndex;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class SystemDataReadFilter extends AspectReadFilter {

  @Nonnull
  @Getter
  @Setter
  @Accessors(chain = true)
  private AspectPluginConfig config;

  @Override
  public boolean isAllowed(
      @Nonnull final AspectReadContext context, @Nonnull final EntityRegistry entityRegistry) {
    final String entityType = context.getUrn().getEntityType();
    final SystemDataPolicyIndex policy = SystemDataPolicy.index(entityRegistry);
    if (context.getIntent() == ReadIntent.EXISTS) {
      return policy.isExistsEligible(entityType, context.getAspectName());
    }
    return policy.isReadEligible(entityType, context.getAspectName());
  }
}
