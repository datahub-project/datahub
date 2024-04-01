package com.linkedin.datahub.graphql.types.aspect;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaMetadataMapper;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AspectMapper {

  public static final AspectMapper INSTANCE = new AspectMapper();

  public static Aspect map(
      @Nullable QueryContext context,
      @Nonnull final EnvelopedAspect aspect,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, aspect, entityUrn);
  }

  public Aspect apply(
      @Nullable QueryContext context,
      @Nonnull final EnvelopedAspect aspect,
      @Nonnull final Urn entityUrn) {
    if (Constants.SCHEMA_METADATA_ASPECT_NAME.equals(aspect.getName())) {
      return SchemaMetadataMapper.map(context, aspect, entityUrn);
    }
    return null;
  }
}
