package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class DirectChildrenResult {
  @Nonnull Set<Urn> childUrns;
  boolean truncated;
}
