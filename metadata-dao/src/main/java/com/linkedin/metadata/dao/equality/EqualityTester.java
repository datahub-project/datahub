package com.linkedin.metadata.dao.equality;

import com.linkedin.data.template.DataTemplate;
import javax.annotation.Nonnull;


/**
 * An interface for testing equality between two objects of the same type.
 */
public interface EqualityTester<T extends DataTemplate> {

  boolean equals(@Nonnull T o1, @Nonnull T o2);
}
