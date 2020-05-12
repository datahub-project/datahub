package com.linkedin.metadata.dao.equality;

import com.linkedin.data.template.DataTemplate;
import com.linkedin.data.template.DataTemplateUtil;
import javax.annotation.Nonnull;

/**
 * A {@link EqualityTester} that uses {@link DataTemplateUtil#areEqual(DataTemplate, DataTemplate)} to check for
 * semantic equality.
 */
public class DefaultEqualityTester<T extends DataTemplate> implements EqualityTester<T> {

  /**
   * Creates a new instance of {@link DefaultEqualityTester}.
   */
  public static <CLASS extends DataTemplate> DefaultEqualityTester<CLASS> newInstance() {
    return new DefaultEqualityTester<>();
  }

  @Override
  public boolean equals(@Nonnull T o1, @Nonnull T o2) {
    return DataTemplateUtil.areEqual(o1, o2);
  }
}
