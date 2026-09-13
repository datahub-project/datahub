package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.structured.StructuredProperties;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Provides field resolver for structured properties given entitySpec */
@Slf4j
@RequiredArgsConstructor
public class StructuredPropertyFieldResolverProvider implements EntityFieldResolverProvider {

  private final SystemEntityClient _entityClient;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.STRUCTURED_PROPERTY);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(
        entitySpec, spec -> getStructuredProperties(opContext, spec));
  }

  private FieldResolver.FieldValue getStructuredProperties(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {

    EnvelopedAspect structuredPropertiesAspect;
    try {
      if (entitySpec.getEntity().isEmpty()) {
        return FieldResolver.emptyFieldValue();
      }

      Urn entityUrn = UrnUtils.getUrn(entitySpec.getEntity());

      EntityResponse response =
          _entityClient.getV2(
              opContext,
              entityUrn.getEntityType(),
              entityUrn,
              Collections.singleton(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME));
      if (response == null
          || !response.getAspects().containsKey(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME)) {
        return FieldResolver.emptyFieldValue();
      }
      structuredPropertiesAspect =
          response.getAspects().get(Constants.STRUCTURED_PROPERTIES_ASPECT_NAME);
    } catch (Exception e) {
      log.error(
          "Error while retrieving structured properties aspect for entitySpec {}", entitySpec, e);
      return FieldResolver.emptyFieldValue();
    }

    StructuredProperties structuredProperties =
        new StructuredProperties(structuredPropertiesAspect.getValue().data());

    // Map of propertyUrn -> Set of values
    java.util.Map<String, java.util.Set<String>> propertyMap = new java.util.HashMap<>();
    structuredProperties.getProperties().stream()
        .forEach(
            property -> {
              try {
                java.util.Set<String> values =
                    property.getValues().stream()
                        .filter(v -> v != null)
                        .map(
                            v -> {
                              try {
                                if (v.isString()) {
                                  return v.getString();
                                } else if (v.isDouble()) {
                                  return String.valueOf(v.getDouble());
                                } else {
                                  log.warn(
                                      "Unexpected union type for structured property value: {}",
                                      property.getPropertyUrn());
                                  return null;
                                }
                              } catch (Exception e) {
                                log.warn(
                                    "Failed to extract value from structured property: {}",
                                    property.getPropertyUrn(),
                                    e);
                                return null;
                              }
                            })
                        .filter(v -> v != null)
                        .collect(Collectors.toSet());
                propertyMap.put(property.getPropertyUrn().toString(), values);
              } catch (Exception e) {
                log.warn("Failed to process structured property: {}", property.getPropertyUrn(), e);
              }
            });

    return FieldResolver.FieldValue.builder()
        .values(java.util.Collections.emptySet())
        .structuredPropertyValues(propertyMap)
        .build();
  }
}
