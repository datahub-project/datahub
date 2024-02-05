package com.linkedin.metadata.aspect.patch.builder.subtypesupport;

import com.linkedin.metadata.aspect.patch.builder.AbstractMultiFieldPatchBuilder;
import java.util.Map;
import javax.annotation.Nonnull;

/** Interface to implement if an aspect supports custom properties changes */
public interface CustomPropertiesPatchBuilderSupport<T extends AbstractMultiFieldPatchBuilder<T>> {

  /**
   * Adds a custom property
   *
   * @param key
   * @param value
   * @return
   */
  T addCustomProperty(@Nonnull String key, @Nonnull String value);

  /**
   * Removes a custom property
   *
   * @param key
   * @return
   */
  T removeCustomProperty(@Nonnull String key);

  /**
   * Fully replace the custom properties
   *
   * @param properties
   * @return
   */
  T setCustomProperties(Map<String, String> properties);
}
