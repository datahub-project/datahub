package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaMetadataMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.aspect.VersionedAspect;
import javax.annotation.Nonnull;


public class AspectMapper implements ModelMapper<VersionedAspect, Aspect> {

  public static final AspectMapper INSTANCE = new AspectMapper();

  public static Aspect map(@Nonnull final VersionedAspect restliAspect) {
    return INSTANCE.apply(restliAspect);
  }

  @Override
  public Aspect apply(@Nonnull final VersionedAspect restliAspect) {
    if (restliAspect.getAspect().isSchemaMetadata()) {
      return SchemaMetadataMapper.map(restliAspect);
    }
    return null;
  }
}
