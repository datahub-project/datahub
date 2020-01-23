package com.linkedin.metadata.dao.equality;

import javax.annotation.Nonnull;


/**
 * An interface for testing equality between two objects of the same type.
 */
public interface EqualityTester<T> {

  boolean equals(@Nonnull T o1, @Nonnull T o2);
}
