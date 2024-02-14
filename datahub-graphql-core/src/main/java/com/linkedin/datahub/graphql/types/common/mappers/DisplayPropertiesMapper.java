package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.DisplayProperties;
import com.linkedin.datahub.graphql.generated.IconLibrary;
import com.linkedin.datahub.graphql.generated.IconProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

public class DisplayPropertiesMapper
    implements ModelMapper<com.linkedin.common.DisplayProperties, DisplayProperties> {
  public static final DisplayPropertiesMapper INSTANCE = new DisplayPropertiesMapper();

  public static DisplayProperties map(com.linkedin.common.DisplayProperties input) {
    return INSTANCE.apply(input);
  }

  @Override
  public DisplayProperties apply(com.linkedin.common.DisplayProperties input) {
    final DisplayProperties result = new DisplayProperties();

    if (input.hasColorHex()) {
      result.setColorHex(input.getColorHex());
    }
    if (input.hasIcon()) {
      final com.linkedin.common.IconProperties iconPropertiesInput = input.getIcon();
      if (iconPropertiesInput != null) {
        final IconProperties iconPropertiesResult = new IconProperties();
        if (iconPropertiesInput.hasIconLibrary()) {
          iconPropertiesResult.setIconLibrary(
              IconLibrary.valueOf(iconPropertiesInput.getIconLibrary().toString()));
        }
        if (iconPropertiesInput.hasName()) {
          iconPropertiesResult.setName(iconPropertiesInput.getName());
        }
        if (iconPropertiesInput.hasStyle()) {
          iconPropertiesResult.setStyle(iconPropertiesInput.getStyle());
        }
        result.setIcon(iconPropertiesResult);
      }
    }

    return result;
  }
}
