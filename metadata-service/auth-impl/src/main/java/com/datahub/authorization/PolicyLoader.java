package com.datahub.authorization;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * Loads every policy from the primary store in one consistent read.
 *
 * <p>Each result must carry the policy URN: {@link PolicySnapshot} uses it to break ties in the
 * policy order.
 */
public interface PolicyLoader {

  @Nonnull
  List<PolicyFetcher.Policy> loadAll();
}
