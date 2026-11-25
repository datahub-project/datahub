package com.datahub.authorization.fieldresolverprovider;

import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.FieldResolver;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.aspect.validation.FabricTypeNameFields;
import com.linkedin.metadata.utils.EntityKeyUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves the ENV policy field to the environment an entity holds in its FabricTypeName fields. A
 * key field is read from the URN with no I/O; an ordinary aspect field is read by fetching the
 * aspect, and only when a policy actually filters on ENV (field resolvers are lazy). Entity types
 * without an environment resolve to no value.
 */
@Slf4j
@RequiredArgsConstructor
public class EnvFieldResolverProvider implements EntityFieldResolverProvider {

  /** The FabricTypeName fields of the registry, from {@link FabricTypeNameFields#discover}. */
  @Nonnull private final Supplier<List<FabricTypeNameFields.Field>> fields;

  @Nonnull private final SystemEntityClient entityClient;

  @Override
  public List<EntityFieldType> getFieldTypes() {
    return Collections.singletonList(EntityFieldType.ENV);
  }

  @Override
  public FieldResolver getFieldResolver(
      @Nonnull OperationContext opContext, EntitySpec entitySpec) {
    return FieldResolver.getResolverFromFunction(entitySpec, spec -> envs(opContext, spec));
  }

  private FieldResolver.FieldValue envs(
      @Nonnull OperationContext opContext, @Nonnull EntitySpec entitySpec) {
    if (entitySpec.getEntity().isEmpty()) {
      return FieldResolver.emptyFieldValue();
    }
    List<FabricTypeNameFields.Field> entityFields =
        fields.get().stream()
            .filter(field -> field.getEntityName().equals(entitySpec.getType()))
            .collect(Collectors.toList());
    if (entityFields.isEmpty()) {
      return FieldResolver.emptyFieldValue();
    }
    try {
      Urn urn = UrnUtils.getUrn(entitySpec.getEntity());
      Set<String> values = new LinkedHashSet<>();
      Set<String> aspectNames = new LinkedHashSet<>();
      for (FabricTypeNameFields.Field field : entityFields) {
        if (field.isKeyAspect()) {
          // The URN's type comes from data; an entity type this registry does not know has no env.
          Optional<com.linkedin.metadata.models.EntitySpec> urnSpec =
              opContext.getEntityRegistry().findEntitySpec(urn.getEntityType());
          if (urnSpec.isEmpty()) {
            continue;
          }
          String value =
              EntityKeyUtils.convertUrnToEntityKey(urn, urnSpec.get().getKeyAspectSpec())
                  .data()
                  .getString(field.getFieldName());
          if (value != null) {
            values.add(value);
          }
        } else {
          aspectNames.add(field.getAspectName());
        }
      }
      if (!aspectNames.isEmpty()) {
        EntityResponse response =
            entityClient.getV2(opContext, urn.getEntityType(), urn, aspectNames);
        if (response != null) {
          for (FabricTypeNameFields.Field field : entityFields) {
            EnvelopedAspect aspect = response.getAspects().get(field.getAspectName());
            String value =
                aspect == null ? null : aspect.getValue().data().getString(field.getFieldName());
            if (!field.isKeyAspect() && value != null) {
              values.add(value);
            }
          }
        }
      }
      return FieldResolver.FieldValue.builder().values(values).build();
    } catch (Exception e) {
      log.error(
          "Error while resolving ENV for entitySpec {}; returning empty field value for this"
              + " policy criterion",
          entitySpec,
          e);
      return FieldResolver.emptyFieldValue();
    }
  }
}
