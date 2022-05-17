package com.linkedin.datahub.upgrade.propagate.comparator;

import com.linkedin.datahub.upgrade.propagate.EntityDetails;
import com.linkedin.schema.SchemaField;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


public class SchemaBasedMatcher implements EntityMatcher {
  private static final double THRESHOLD = 0.5;

  @Nullable
  public EntityMatchResult match(EntityDetails original, Collection<EntityDetails> others) {
    if (original.getSchemaMetadata() == null || original.getSchemaMetadata().getFields().size() <= 2) {
      return null;
    }

    Set<String> fieldPaths =
        original.getSchemaMetadata().getFields().stream().map(SchemaField::getFieldPath).collect(Collectors.toSet());

    Map<String, String> simplifiedFieldPaths = fieldPaths.stream()
        .collect(Collectors.toMap(this::processFieldPath, Function.identity(),
            (a1, a2) -> a1.length() > a2.length() ? a1 : a2));

    int minDiff = fieldPaths.size();
    int allowedDiff = (int) (fieldPaths.size() * (1 - THRESHOLD));

    EntityDetails matchedDetails = null;
    Map<String, String> finalMatchedFields = null;

    for (EntityDetails other : others) {
      if (other.getSchemaMetadata() == null || other.getSchemaMetadata().getFields().size() <= 2) {
        continue;
      }
      int numDiffFields = Math.max(0, fieldPaths.size() - other.getSchemaMetadata().getFields().size());
      Map<String, String> matchedFields = new HashMap<>();
      for (SchemaField field : other.getSchemaMetadata().getFields()) {
        if (fieldPaths.contains(field.getFieldPath())) {
          matchedFields.put(field.getFieldPath(), field.getFieldPath());
          continue;
        }
        String simplifiedFieldPath = processFieldPath(field.getFieldPath());
        if (simplifiedFieldPaths.containsKey(simplifiedFieldPath)) {
          matchedFields.put(simplifiedFieldPaths.get(simplifiedFieldPath), field.getFieldPath());
          continue;
        }

        numDiffFields++;
        if (numDiffFields > allowedDiff) {
          break;
        }
      }

      if (numDiffFields < minDiff) {
        minDiff = numDiffFields;
        if (numDiffFields <= allowedDiff) {
          matchedDetails = other;
          finalMatchedFields = matchedFields;
        }
      }
    }

    if (matchedDetails == null) {
      return null;
    }

    return EntityMatchResult.builder()
        .matchedEntity(matchedDetails)
        .similarityScore(1.0 - 1.0 * minDiff / fieldPaths.size())
        .matchingFields(finalMatchedFields)
        .build();
  }

  private String processFieldPath(String fieldPath) {
    return fieldPath.toLowerCase().replaceAll("\\p{Punct}", "");
  }
}
