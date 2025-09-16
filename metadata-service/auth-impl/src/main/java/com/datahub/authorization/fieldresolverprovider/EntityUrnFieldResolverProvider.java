package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;

/** Provides field resolver for entity urn given entitySpec */
public class EntityUrnFieldResolverProvider implements EntityFieldResolverProvider {

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return ImmutableList.of(EntityFieldType.URN, EntityFieldType.RESOURCE_URN);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, this::getUrn);
  }

  private FieldResolver.FieldValue getUrn(EntitySpec entitySpec) {
    if (entitySpec.getEntity().isEmpty()) {
      return FieldResolver.emptyFieldValue();
    }
    System.out.println("~~~~~~~~~~~~~~~~~~HERE~~~~~~~~~~~~~~~~~~~");
    System.out.println(entitySpec.getEntity());
    System.out.println("~~~~~~~~~~~~~~~~~~HERE~~~~~~~~~~~~~~~~~~~");
    // could simply add parent urns if this is a domain? i think yes maybe
    if (!entitySpec.getEntity().equals("urn:li:domain:marketing")) {
      return FieldResolver.FieldValue.builder()
          .values(Set.of(entitySpec.getEntity(), "urn:li:domain:marketing"))
          .build();
    }
    return FieldResolver.FieldValue.builder().values(Set.of(entitySpec.getEntity())).build();
  }
}
