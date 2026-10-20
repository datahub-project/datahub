package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DeclaredKey;
import com.linkedin.dataset.DeclaredKeys;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Validates writes of the {@code declaredKeys} aspect across every write API: keys must not be
 * empty or contain duplicate fields, business key names must be unique, and every field path must
 * exist in the dataset's {@code schemaMetadata} when the dataset has one.
 *
 * <p>Runs pre-commit so PATCH writes are checked on the merged aspect.
 */
@Setter
@Getter
@Accessors(chain = true)
public class DeclaredKeysValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    final ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    final List<ChangeMCP> items =
        changeMCPs.stream()
            .filter(item -> item.getAspect(DeclaredKeys.class) != null)
            .collect(Collectors.toList());
    if (items.isEmpty()) {
      return exceptions.streamAllExceptions();
    }

    // ponytail: reads the stored schema, so a schemaMetadata written in the same batch is not seen.
    final Map<Urn, Map<String, Aspect>> schemas =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObjects(
                operationContext,
                items.stream().map(ChangeMCP::getUrn).collect(Collectors.toSet()),
                Set.of(SCHEMA_METADATA_ASPECT_NAME));

    for (ChangeMCP item : items) {
      final Consumer<String> fail = message -> exceptions.addException(item, message);
      final Set<String> schemaFields =
          readFieldPaths(schemas.getOrDefault(item.getUrn(), Map.of()));
      final DeclaredKeys keys = item.getAspect(DeclaredKeys.class);

      if (keys.hasPrimaryKey()) {
        validateKey("primary key", keys.getPrimaryKey(), schemaFields, fail);
      }
      if (keys.hasBusinessKeys()) {
        final Set<String> names = new HashSet<>();
        for (DeclaredKey key : keys.getBusinessKeys()) {
          final String label = String.format("business key '%s'", key.getName());
          if (key.getName().isBlank()) {
            fail.accept("Business key names must not be blank");
          } else if (!names.add(key.getName())) {
            fail.accept(String.format("Duplicate %s", label));
          }
          validateKey(label, key.getFields(), schemaFields, fail);
        }
      }
    }
    return exceptions.streamAllExceptions();
  }

  private static void validateKey(
      @Nonnull final String label,
      @Nonnull final List<String> fields,
      @Nullable final Set<String> schemaFields,
      @Nonnull final Consumer<String> fail) {
    if (fields.isEmpty()) {
      fail.accept(String.format("The %s must contain at least one field", label));
      return;
    }
    if (new HashSet<>(fields).size() != fields.size()) {
      fail.accept(String.format("The %s contains duplicate fields: %s", label, fields));
    }
    if (schemaFields != null) {
      final List<String> missing =
          fields.stream().filter(f -> !schemaFields.contains(f)).collect(Collectors.toList());
      if (!missing.isEmpty()) {
        fail.accept(
            String.format("The %s references fields not in the schema: %s", label, missing));
      }
    }
  }

  /** Field paths of the stored schema, or null when the dataset has no schema to check against. */
  @Nullable
  private static Set<String> readFieldPaths(@Nonnull final Map<String, Aspect> aspects) {
    final Aspect aspect = aspects.get(SCHEMA_METADATA_ASPECT_NAME);
    if (aspect == null) {
      return null;
    }
    final SchemaMetadata schema = new SchemaMetadata(aspect.data());
    if (!schema.hasFields() || schema.getFields().isEmpty()) {
      return null;
    }
    return schema.getFields().stream().map(SchemaField::getFieldPath).collect(Collectors.toSet());
  }
}
