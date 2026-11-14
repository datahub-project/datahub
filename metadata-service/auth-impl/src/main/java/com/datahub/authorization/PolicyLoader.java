package com.datahub.authorization;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Loads every policy from the primary store in one consistent read.
 *
 * <p>Each result must carry the policy URN: {@link PolicySnapshot} uses it to break ties in the
 * policy order.
 */
public interface PolicyLoader {

  @Nonnull
  Result loadAll();

  /**
   * @param policies the policies that were read
   * @param skipped the number of stored policies that could not be read and are not in {@code
   *     policies}
   */
  record Result(@Nonnull List<PolicyFetcher.Policy> policies, int skipped) {
    public Result {
      Objects.requireNonNull(policies);
    }

    public static Result of(@Nonnull List<PolicyFetcher.Policy> policies) {
      return new Result(policies, 0);
    }
  }
}
