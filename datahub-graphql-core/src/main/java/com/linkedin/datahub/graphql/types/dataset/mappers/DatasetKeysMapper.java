package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.generated.DatasetBusinessKey;
import com.linkedin.datahub.graphql.generated.DatasetKeys;
import com.linkedin.datahub.graphql.generated.PrimaryKeySource;
import com.linkedin.dataset.DeclaredKeys;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Resolves the keys of a dataset from the keys declared by its source ({@code schemaMetadata}) and
 * the keys declared by users ({@code declaredKeys}).
 *
 * <p>The source is the technical truth for the physical key: a source primary key always wins. A
 * declared primary key only takes effect when the source declares none, and a declared key that
 * differs from the source key is reported as a conflict instead of being applied.
 */
public class DatasetKeysMapper {

  private DatasetKeysMapper() {}

  @Nonnull
  public static DatasetKeys map(
      @Nullable final SchemaMetadata schemaMetadata, @Nullable final DeclaredKeys declaredKeys) {
    final List<String> sourceKey = sourcePrimaryKey(schemaMetadata);
    final List<String> declaredKey =
        declaredKeys != null && declaredKeys.hasPrimaryKey()
            ? new ArrayList<>(declaredKeys.getPrimaryKey())
            : List.of();

    final DatasetKeys result = new DatasetKeys();
    if (!sourceKey.isEmpty()) {
      result.setEffectivePrimaryKey(sourceKey);
      result.setPrimaryKeySource(PrimaryKeySource.SOURCE);
    } else if (!declaredKey.isEmpty()) {
      result.setEffectivePrimaryKey(declaredKey);
      result.setPrimaryKeySource(PrimaryKeySource.DECLARED);
    } else {
      result.setEffectivePrimaryKey(List.of());
      result.setPrimaryKeySource(PrimaryKeySource.NONE);
    }
    result.setDeclaredPrimaryKey(declaredKey.isEmpty() ? null : declaredKey);
    result.setConflict(
        !sourceKey.isEmpty()
            && !declaredKey.isEmpty()
            && !new HashSet<>(sourceKey).equals(new HashSet<>(declaredKey)));

    if (declaredKeys != null && declaredKeys.hasBusinessKeys()) {
      result.setBusinessKeys(
          declaredKeys.getBusinessKeys().stream()
              .map(
                  key -> {
                    final DatasetBusinessKey businessKey = new DatasetBusinessKey();
                    businessKey.setName(key.getName());
                    businessKey.setFields(new ArrayList<>(key.getFields()));
                    businessKey.setDescription(key.getDescription());
                    return businessKey;
                  })
              .collect(Collectors.toList()));
    }
    return result;
  }

  /** {@code schemaMetadata.primaryKeys} plus every field flagged {@code isPartOfKey}, in order. */
  @Nonnull
  private static List<String> sourcePrimaryKey(@Nullable final SchemaMetadata schemaMetadata) {
    if (schemaMetadata == null) {
      return List.of();
    }
    final Set<String> key = new LinkedHashSet<>();
    if (schemaMetadata.hasPrimaryKeys()) {
      key.addAll(schemaMetadata.getPrimaryKeys());
    }
    if (schemaMetadata.hasFields()) {
      schemaMetadata.getFields().stream()
          .filter(field -> Boolean.TRUE.equals(field.isIsPartOfKey()))
          .map(SchemaField::getFieldPath)
          .forEach(key::add);
    }
    return new ArrayList<>(key);
  }
}
