package com.linkedin.util;

import com.linkedin.common.FabricType;
import com.linkedin.data.template.Custom;
import com.linkedin.data.template.DirectCoercer;
import com.linkedin.data.template.TemplateOutputCastException;

/**
 * Binds the {@code FabricTypeName} string typeref to {@link FabricType} in generated Java, so
 * accessors keep the enum type while the stored value stays a string. A stored value that is not a
 * built-in symbol (a custom fabric type) reads as {@code $UNKNOWN}; {@code $UNKNOWN} itself can't
 * be written, since that would replace the real value.
 */
public class FabricTypeNameCoercer implements DirectCoercer<FabricType> {
  static {
    Custom.registerCoercer(new FabricTypeNameCoercer(), FabricType.class);
  }

  @Override
  public Object coerceInput(FabricType object) throws ClassCastException {
    if (object == FabricType.$UNKNOWN) {
      throw new ClassCastException("$UNKNOWN can't be written as a FabricType value");
    }
    return object.name();
  }

  @Override
  public FabricType coerceOutput(Object object) throws TemplateOutputCastException {
    if (!(object instanceof String)) {
      throw new TemplateOutputCastException("Expected a string for FabricType, got " + object);
    }
    try {
      return FabricType.valueOf((String) object);
    } catch (IllegalArgumentException e) {
      return FabricType.$UNKNOWN;
    }
  }
}
