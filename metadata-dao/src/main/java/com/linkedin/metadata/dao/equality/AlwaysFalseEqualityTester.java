package com.linkedin.metadata.dao.equality;

import javax.annotation.Nonnull;


/**
 * A {@link EqualityTester} that always returns false.
 */
public class AlwaysFalseEqualityTester<T> implements EqualityTester<T> {

  /**
   * Creates a new instance of {@link AlwaysFalseEqualityTester}.
   */
  public static <CLASS> AlwaysFalseEqualityTester<CLASS> newInstance() {
    return new AlwaysFalseEqualityTester<CLASS>();
  }

  @Override
  public boolean equals(@Nonnull T o1, @Nonnull T o2) {
    return false;
  }
}
