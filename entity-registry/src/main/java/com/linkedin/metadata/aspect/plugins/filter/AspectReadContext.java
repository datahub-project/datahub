package com.linkedin.metadata.aspect.plugins.filter;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class AspectReadContext {
  @Nonnull Urn urn;
  @Nonnull String aspectName;
  @Nonnull ReadIntent intent;
  @Nullable EntityAspect entityAspect;
}
