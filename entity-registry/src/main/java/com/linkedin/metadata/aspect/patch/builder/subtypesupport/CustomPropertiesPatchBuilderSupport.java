/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
