package io.datahubproject.openapi.v1.models.registry;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EntitySpecDto {
  private String name;
  private EntityAnnotationDto entityAnnotation;
  private String keyAspectName;
  private AspectSpecDto keyAspectSpec;
  private List<AspectSpecDto> aspectSpecs;
  private Map<String, AspectSpecDto> aspectSpecMap;

  private List<FieldSpecDto> searchableFieldSpecs;
  private Map<String, Set<String>> searchableFieldTypes;
  private List<FieldSpecDto> searchScoreFieldSpecs;
  private List<FieldSpecDto> relationshipFieldSpecs;
  private List<FieldSpecDto> searchableRefFieldSpecs;

  // Schema information
  private String snapshotSchemaName;
  private String aspectTyperefSchemaName;

  public static EntitySpecDto fromEntitySpec(EntitySpec entitySpec) {
    if (entitySpec == null) {
      return null;
    }

    return EntitySpecDto.builder()
        .name(entitySpec.getName())
        .entityAnnotation(
            EntityAnnotationDto.fromEntityAnnotation(entitySpec.getEntityAnnotation()))
        .keyAspectName(entitySpec.getKeyAspectName())
        .keyAspectSpec(AspectSpecDto.fromAspectSpec(entitySpec.getKeyAspectSpec()))
        .aspectSpecs(
            entitySpec.getAspectSpecs().stream()
                .map(AspectSpecDto::fromAspectSpec)
                .collect(Collectors.toList()))
        .aspectSpecMap(
            entitySpec.getAspectSpecMap().entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> AspectSpecDto.fromAspectSpec(entry.getValue()))))
        .searchableFieldSpecs(
            entitySpec.getSearchableFieldSpecs().stream()
                .map(FieldSpecDto::fromFieldSpec)
                .collect(Collectors.toList()))
        .searchableFieldTypes(convertSearchableFieldTypes(entitySpec.getSearchableFieldTypes()))
        .searchScoreFieldSpecs(
            entitySpec.getSearchScoreFieldSpecs().stream()
                .map(FieldSpecDto::fromFieldSpec)
                .collect(Collectors.toList()))
        .relationshipFieldSpecs(
            entitySpec.getRelationshipFieldSpecs().stream()
                .map(FieldSpecDto::fromFieldSpec)
                .collect(Collectors.toList()))
        .searchableRefFieldSpecs(
            entitySpec.getSearchableRefFieldSpecs().stream()
                .map(FieldSpecDto::fromFieldSpec)
                .collect(Collectors.toList()))

        // Schema information - handle potential UnsupportedOperationException
        .snapshotSchemaName(getSnapshotSchemaName(entitySpec))
        .aspectTyperefSchemaName(getAspectTyperefSchemaName(entitySpec))
        .build();
  }

  private static Map<String, Set<String>> convertSearchableFieldTypes(
      Map<String, Set<SearchableAnnotation.FieldType>> fieldTypes) {
    if (fieldTypes == null) {
      return new HashMap<>();
    }

    return fieldTypes.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().stream().map(Enum::name).collect(Collectors.toSet())));
  }

  private static String getSnapshotSchemaName(EntitySpec entitySpec) {
    try {
      return entitySpec.getSnapshotSchema() != null
          ? entitySpec.getSnapshotSchema().getFullName()
          : null;
    } catch (UnsupportedOperationException e) {
      // Config-based entities don't have snapshot schemas
      return null;
    }
  }

  private static String getAspectTyperefSchemaName(EntitySpec entitySpec) {
    try {
      return entitySpec.getAspectTyperefSchema() != null
          ? entitySpec.getAspectTyperefSchema().getFullName()
          : null;
    } catch (UnsupportedOperationException e) {
      // Config-based entities don't have aspect typeref schemas
      return null;
    }
  }
}
