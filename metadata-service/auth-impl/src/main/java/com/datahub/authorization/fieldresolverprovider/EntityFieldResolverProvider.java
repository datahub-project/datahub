package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
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
}
