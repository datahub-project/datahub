package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaMetadataMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.aspect.AspectWithMetadata;
import javax.annotation.Nonnull;


public class AspectMapper implements ModelMapper<AspectWithMetadata, Aspect> {

  public static final AspectMapper INSTANCE = new AspectMapper();

  public static Aspect map(@Nonnull final AspectWithMetadata restliAspect) {
    return INSTANCE.apply(restliAspect);
  }

  @Override
  public Aspect apply(@Nonnull final AspectWithMetadata restliAspect) {
    if (restliAspect.getAspect().isSchemaMetadata()) {
      return SchemaMetadataMapper.map(restliAspect);
    }
    return null;
  }
}
