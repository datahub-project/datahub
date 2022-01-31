package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaMetadataMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;


public class AspectMapper implements ModelMapper<EnvelopedAspect, Aspect> {

  public static final AspectMapper INSTANCE = new AspectMapper();

  public static Aspect map(@Nonnull final EnvelopedAspect aspect) {
    return INSTANCE.apply(aspect);
  }

  @Override
  public Aspect apply(@Nonnull final EnvelopedAspect aspect) {
    if (Constants.SCHEMA_METADATA_ASPECT_NAME.equals(aspect.getName())) {
      return SchemaMetadataMapper.map(aspect);
    }
    return null;
  }
}
