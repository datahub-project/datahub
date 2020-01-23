package com.linkedin.metadata.dao.equality;

import javax.annotation.Nonnull;

/**
 * A {@link EqualityTester} that relies on the default implementation of {@link Object#equals(Object)}.
 */
public class DefaultEqualityTester<T> implements EqualityTester<T> {

  /**
   * Creates a new instance of {@link DefaultEqualityTester}.
   */
  public static <CLASS> DefaultEqualityTester<CLASS> newInstance() {
    return new DefaultEqualityTester<CLASS>();
  }

  @Override
  public boolean equals(@Nonnull T o1, @Nonnull T o2) {
    return o1.equals(o2);
  }
}
