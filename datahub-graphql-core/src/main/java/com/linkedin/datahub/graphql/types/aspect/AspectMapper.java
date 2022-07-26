package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaMetadataMapper;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;


public class AspectMapper {

  public static final AspectMapper INSTANCE = new AspectMapper();

  public static Aspect map(@Nonnull final EnvelopedAspect aspect, @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(aspect, entityUrn);
  }

  public Aspect apply(@Nonnull final EnvelopedAspect aspect, @Nonnull final Urn entityUrn) {
    if (Constants.SCHEMA_METADATA_ASPECT_NAME.equals(aspect.getName())) {
      return SchemaMetadataMapper.map(aspect, entityUrn);
    }
    return null;
  }
}
