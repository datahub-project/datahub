//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.linkedin.metadata.entity.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.message.Message;
import com.linkedin.data.schema.DataSchema.Type;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.validator.Validator;
import com.linkedin.data.schema.validator.ValidatorContext;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Setter;


public class EntityRegistryUrnValidator implements Validator {

  private final EntityRegistry _entityRegistry;
  @Setter
  private EntitySpec currentEntitySpec = null;

  public EntityRegistryUrnValidator(EntityRegistry entityRegistry) {
    _entityRegistry = entityRegistry;
  }

  public void validate(ValidatorContext context) {
    if (currentEntitySpec == null) {
      throw new IllegalStateException("Current entity spec must be set");
    }
    validateUrnField(context);
  }

  protected void validateUrnField(ValidatorContext context) {
    if (Type.TYPEREF.equals(context.dataElement().getSchema().getType()) && ((NamedDataSchema) context.dataElement()
        .getSchema()).getName().endsWith("Urn")) {
      try {
        // Validate Urn matches field type and that it generates a valid key
        String urnStr = (String) context.dataElement().getValue();
        Urn urn = Urn.createFromString(urnStr);
        EntitySpec entitySpec = _entityRegistry.getEntitySpec(urn.getEntityType());
        RecordTemplate entityKey = EntityKeyUtils.convertUrnToEntityKey(urn,
            entitySpec.getKeyAspectSpec());
        NamedDataSchema namedDataSchema = ((NamedDataSchema) context.dataElement().getSchema());
        Class<? extends Urn> urnClass;
        try {
          String schemaName = ((Map<String, String>) namedDataSchema.getProperties().get("java")).get("class");
          urnClass = (Class<? extends Urn>) Class.forName(schemaName);
          urnClass.getDeclaredMethod("createFromString", String.class).invoke(null, urnStr);
        } catch (ClassNotFoundException | ClassCastException | NoSuchMethodException e) {
          throw new IllegalArgumentException("Unrecognized Urn class: " + namedDataSchema.getName(), e);
        } catch (InvocationTargetException | IllegalAccessException e) {
          throw new IllegalArgumentException("Unable to instantiate urn type: " + namedDataSchema.getName() + " with urn: " + urnStr, e);
        }

        // Validate generic Urn is valid entity type for relationship destination
        PathSpec fieldPath = context.dataElement().getSchemaPathSpec();
        List<RelationshipFieldSpec> relationshipSpecs = currentEntitySpec.getRelationshipFieldSpecs().stream().filter(relationshipFieldSpec ->
                relationshipFieldSpec.getPath().equals(fieldPath))
            .collect(Collectors.toList());
        if (!relationshipSpecs.isEmpty()) {
          for (RelationshipFieldSpec relationshipFieldSpec : relationshipSpecs) {
            boolean isValidDestination = relationshipFieldSpec.getValidDestinationTypes().stream()
                .anyMatch(destinationType -> destinationType.equals(urn.getEntityType()));
            if (!isValidDestination) {
              throw new IllegalArgumentException(
                  "Entity type for urn: " + urn + " is not a valid destination for field path: " + fieldPath);
            }
          }
        }
      } catch (URISyntaxException | IllegalArgumentException e) {
        context.addResult(new Message(context.dataElement().path(), "\"Provided urn %s\" is invalid: %s",
            context.dataElement().getValue(), e.getMessage()));
        context.setHasFix(false);
      }
    }
  }
}
