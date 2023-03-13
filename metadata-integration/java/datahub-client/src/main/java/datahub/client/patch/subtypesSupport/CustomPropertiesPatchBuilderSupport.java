package datahub.client.patch.subtypesSupport;

import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.common.CustomPropertiesPatchBuilder;


/**
 * Interface to implement if an aspect supports custom properties changes
 */
public interface CustomPropertiesPatchBuilderSupport<T extends AbstractMultiFieldPatchBuilder<T>> {

  /**
   * Overrides the default patch builder for an aspect to specifically target the custom properties field.
   * This allows setting of individual properties without overriding the entire map.
   */
  CustomPropertiesPatchBuilder<T> customPropertiesPatchBuilder();
}
