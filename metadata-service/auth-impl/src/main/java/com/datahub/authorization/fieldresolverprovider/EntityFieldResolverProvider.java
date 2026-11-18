package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.entity.EntityResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/** Base class for defining a class that provides the field resolver for the given field type */
public interface EntityFieldResolverProvider {

  /**
   * List of fields that this hydrator is hydrating.
   *
   * @return
   */
  List<EntityFieldType> getFieldTypes();

  /** Return resolver for fetching the field values given the entity */
  FieldResolver getFieldResolver(@Nonnull OperationContext opContext, EntitySpec entitySpec);

  /** Aspects of the entity itself that this provider reads to resolve its fields. */
  default Set<String> getAspectNames() {
    return Set.of();
  }

  /**
   * {@link #getFieldResolver(OperationContext, EntitySpec)} reading the aspects named by {@link
   * #getAspectNames()} from {@code prefetched} instead of fetching them.
   */
  default FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext,
      EntitySpec entitySpec,
      @Nonnull EntityResponse prefetched) {
    return getFieldResolver(opContext, entitySpec);
  }
}
