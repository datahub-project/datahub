package com.linkedin.metadata.dao.equality;

import com.linkedin.data.template.DataTemplate;
import javax.annotation.Nonnull;


/**
 * A {@link EqualityTester} that always returns false.
 */
public class AlwaysFalseEqualityTester<T extends DataTemplate> implements EqualityTester<T> {

  /**
   * Creates a new instance of {@link AlwaysFalseEqualityTester}.
   */
  public static <CLASS extends DataTemplate> AlwaysFalseEqualityTester<CLASS> newInstance() {
    return new AlwaysFalseEqualityTester<>();
  }

  @Override
  public boolean equals(@Nonnull T o1, @Nonnull T o2) {
    return false;
  }
}
